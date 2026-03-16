from casa import imfit, split
import glob, multiprocessing, os, datetime, shutil

class ImagingAndFlux:

    def __init__(self, t0, t1, binsz, name, vis, basedir, rec_aperture, channelout, image_size, step):
        self.Tstart = t0
        self.T_End = t1
        self.binsz = binsz
        self.step = step
        self.name = name
        self.vis_file = vis
        self.basedir = basedir
        self.rec_aperture = rec_aperture
        self.channelout = channelout
        self.image_size = image_size
        self.tim_st = t0.hour*3600 + t0.minute*60 + t0.second
        self.tim_end = t1.hour*3600 + t1.minute*60 + t1.second

    ##################################
    # Data slicing using CASA split
    ##################################
    def slice_data(self):
        print("##### CASA/WSClean Imaging Pipeline #####")
        self.n_intervals = ((self.tim_end - self.tim_st) - self.binsz)/self.step
        self.t_1 = self.Tstart - datetime.timedelta(seconds=self.step)
        self.t_2 = self.Tstart + datetime.timedelta(seconds=self.binsz-self.step)

        for i in range(int(self.n_intervals)+1):
            self.t_1 += datetime.timedelta(seconds=self.step)
            self.t_2 += datetime.timedelta(seconds=self.step)
            self.t3 = self.t_1 + (self.t_2 - self.t_1)/2
            self.timerange = f"{self.t_1.time()}~{self.t_2.time()}"
            self.output_vis = f"{self.name}{self.t3.date()}T{self.t3.time()}_secs.ms"

            # determine output directory
            outdir = os.path.join(self.basedir if os.path.isdir(self.basedir) else os.getcwd(),
                                  f"lc_{self.t_1.hour*3600 + self.t_1.minute*60 + self.t_1.second:.0f}_{self.t_2.hour*3600 + self.t_2.minute*60 + self.t_2.second:.0f}")
            os.makedirs(outdir, exist_ok=True)

            # CASA split
            try:
                split(vis=self.vis_file,
                      outputvis=os.path.join(outdir, self.output_vis),
                      datacolumn='data',
                      timerange=self.timerange)
            except Exception as e:
                print(f"Split failed for {self.output_vis}: {e}")

    ##################################
    # Remove corrupted FITS images
    ##################################
    def remove_crpt_image(self, tim_st):
        u, v = tim_st
        target_dir = os.path.join(self.basedir if os.path.isdir(self.basedir) else os.getcwd(),
                                  f"lc_{u:.0f}_{v:.0f}")
        for filename in os.listdir(target_dir):
            if filename.endswith(('-image.fits', '_secs.ms_final.fits')):
                os.remove(os.path.join(target_dir, filename))
                print(f"{filename} deleted")

    ##################################
    # Run WSClean imaging
    ##################################
    def imaging(self, tim_st):
        u, v = tim_st
        target_dir = os.path.join(self.basedir if os.path.isdir(self.basedir) else os.getcwd(),
                                  f"lc_{u:.0f}_{v:.0f}")
        for filename in os.listdir(target_dir):
            if not filename.endswith('_secs.ms'):
                continue

            mslist = glob.glob(os.path.join(target_dir, filename))
            imgname = f"img_{filename}"
            final_image = os.path.join(target_dir, f"Image_{filename}_final.fits")
            if os.path.isfile(final_image):
                print(f"WSClean already performed on {filename}, skipping")
                continue

            syscall = (
                f"wsclean {self.image_size} -scale 1.5asec -niter 30000 -gain 0.1 -mgain 0.85 "
                "-weight briggs -0.7 -datacolumn DATA -auto-threshold 0.3 -auto-mask 5.0 "
                f"-name {imgname} {self.channelout} -fit-spectral-pol 3 -joinchannels -mem 50 "
                + " ".join(mslist)
            )
            print(syscall)
            os.system(syscall)

            # Move outputs to target directory
            for f in glob.glob(f"img_{filename}-MFS-image.fits"):
                shutil.move(f, final_image)
            for fl in os.listdir(os.getcwd()):
                if fl.endswith('-image.fits') and fl.startswith(f'img_{filename}'):
                    shutil.move(fl, os.path.join(target_dir, fl))

    ##################################
    # Extract flux from images
    ##################################
    def Extract_flux(self, tim_st):
        u, v = tim_st
        target_dir = os.path.join(self.basedir if os.path.isdir(self.basedir) else os.getcwd(),
                                  f"lc_{u:.0f}_{v:.0f}")
        path = os.getcwd()

        for filename in os.listdir(target_dir):
            channel = None
            if filename.endswith("_secs.ms_final.fits"):
                channel = ""
            elif "_secs.ms-" in filename and filename.endswith("-image.fits"):
                channel = filename.split("_secs.ms-")[1][:4]

            if channel is None:
                continue

            s = filename.replace(f"_secs.ms{('-'+channel) if channel else ''}-image.fits", '')
            try:
                xfit_A_res = imfit(imagename=os.path.join(target_dir, filename), box=self.rec_aperture)
                xfit_A = xfit_A_res['results']['component0']
                if xfit_A['flux']['value'][0] <= 0:
                    continue
                out_file = f"{path}/lc_{channel+'_' if channel else ''}{self.name}{self.binsz}_secs_bin.txt"
                with open(out_file, 'a') as f:
                    f.write(f"{s} {xfit_A['flux']['value'][0]:2.3e} {xfit_A['flux']['error'][0]:2.3e}\n")
            except Exception as e:
                print(f"Deconvolution failed for {filename}: {e}")

    ##################################
    # Extract flux vs frequency
    ##################################
    def Freq_and_flux(self, tim_st):
        u, v = tim_st
        target_dir = os.path.join(self.basedir if os.path.isdir(self.basedir) else os.getcwd(),
                                  f"lc_{u:.0f}_{v:.0f}")
        path = os.getcwd()
        for filename in os.listdir(target_dir):
            if not (filename.endswith("-image.fits") or filename.endswith("_secs.ms_final.fits")):
                continue
            try:
                xfit_A_res = imfit(imagename=os.path.join(target_dir, filename), box=self.rec_aperture)
                major = xfit_A_res['deconvolved']['component0']['shape']['majoraxis']['value']
                if major <= 1e-7:
                    continue
                xfit_A = xfit_A_res['results']['component0']
                out_file = f"{path}/{self.name}time_avg_spectrum.txt"
                with open(out_file, 'a') as f:
                    f.write(f"{xfit_A['spectrum']['frequency']['m0']['value']:10f} "
                            f"{xfit_A['flux']['value'][0]:2.3e} {xfit_A['flux']['error'][0]:2.3e}\n")
            except Exception as e:
                print(f"Deconvolution failed for {filename}: {e}")
