from casa import imfit, split
import glob
import os
import datetime
import shutil
import subprocess
import logging
from typing import List, Tuple, Optional

# Configure logging (can be adjusted by the user)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class ImagingAndFlux:
    """
    A class to perform time‑sliced imaging and flux extraction from a CASA visibility set.
    It uses CASA's split and imfit, and wsclean for imaging.
    """

    def __init__(self, t0: datetime.datetime, t1: datetime.datetime, binsz: int,
                 name: str, vis: str, basedir: str, rec_aperture: str,
                 channelout: str, image_size: str, step: int,
                 wsclean_params: Optional[dict] = None):
        """
        Parameters
        ----------
        t0, t1 : datetime.datetime
            Start and end time of the observation.
        binsz : int
            Bin size in seconds for each time slice.
        step : int
            Step size in seconds between consecutive slices.
        name : str
            Source name (used in output filenames).
        vis : str
            Path to the input measurement set.
        basedir : str
            Base directory where output subdirectories will be created.
        rec_aperture : str
            Box coordinates for imfit, e.g. 'x1,y1,x2,y2'.
        channelout : str
            wsclean parameter, e.g. '-channels-out 3'.
        image_size : str
            wsclean parameter, e.g. '-size 2192 2192 '.
        wsclean_params : dict, optional
            Additional wsclean parameters as key‑value pairs.
            Default parameters are provided if not given.
        """
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

        # Convert times to seconds since midnight (used for directory naming)
        self.tim_st = self._to_seconds(t0)
        self.tim_end = self._to_seconds(t1)

        # Default wsclean parameters (can be overridden)
        self.wsclean_defaults = {
            '-scale': '1.5asec',
            '-niter': '30000',
            '-gain': '0.1',
            '-mgain': '0.85',
            '-weight': 'briggs -0.7',
            '-datacolumn': 'DATA',
            '-auto-threshold': '0.3',
            '-auto-mask': '5.0',
            '-fit-spectral-pol': '3',
            '-joinchannels': '',
            '-mem': '50'
        }
        if wsclean_params:
            self.wsclean_defaults.update(wsclean_params)

    @staticmethod
    def _to_seconds(dt: datetime.datetime) -> int:
        """Convert a datetime object to seconds since midnight."""
        return dt.hour * 3600 + dt.minute * 60 + dt.second

    def _interval_dir(self, t_start_sec: int, t_end_sec: int) -> str:
        """
        Return the path to the directory for a given time interval.
        The directory is named like 'lc_<start>_<end>'.
        """
        return os.path.join(self.basedir, f'lc_{t_start_sec:.0f}_{t_end_sec:.0f}')

    def _ensure_dir(self, path: str) -> None:
        """Create a directory if it does not exist."""
        try:
            os.makedirs(path, exist_ok=True)
        except OSError as e:
            logging.error(f"Failed to create directory {path}: {e}")
            raise

    def _run_wsclean(self, ms_paths: List[str], outdir: str, imgname: str,
                     filename: str) -> None:
        """
        Run wsclean on the given measurement sets.

        Parameters
        ----------
        ms_paths : list of str
            Full paths to the ms files to be imaged.
        outdir : str
            Directory where outputs should be moved.
        imgname : str
            Base name for wsclean output images.
        filename : str
            Original ms filename (used to construct final image name).
        """
        # Build command
        cmd = ['wsclean']
        # Add image size and other fixed parameters
        cmd.extend(self.image_size.split())
        for key, value in self.wsclean_defaults.items():
            cmd.append(key)
            if value:
                cmd.append(value)
        cmd.extend(['-name', imgname])
        cmd.extend(self.channelout.split())
        cmd.extend(ms_paths)

        logging.info(f"Running wsclean: {' '.join(cmd)}")
        try:
            subprocess.run(cmd, check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as e:
            logging.error(f"wsclean failed: {e.stderr}")
            raise

        # Move the MFS image (the main output) to the target directory
        mfs_image = glob.glob(os.path.join(os.getcwd(), f'{imgname}*-MFS-image.fits'))
        if mfs_image:
            src = mfs_image[0]
            dst = os.path.join(outdir, f'Image_{filename}_final.fits')
            shutil.move(src, dst)
            logging.info(f"Moved {src} -> {dst}")
        else:
            logging.warning(f"No MFS image found for {imgname}")

        # Move any per‑channel images
        for fl in os.listdir(os.getcwd()):
            if fl.endswith('-image.fits') and fl.startswith(imgname):
                src = os.path.join(os.getcwd(), fl)
                dst = os.path.join(outdir, fl)
                shutil.move(src, dst)
                logging.info(f"Moved {src} -> {dst}")

    def _extract_flux_single(self, image_path: str, channel_id: str,
                             outfile_base: str) -> None:
        """
        Extract flux from a single image using CASA's imfit and append to a text file.

        Parameters
        ----------
        image_path : str
            Full path to the FITS image.
        channel_id : str
            Identifier for the channel (e.g., '0000', 'MFS').
        outfile_base : str
            Base name for the output text file (without directory).
            The actual file will be placed in the current working directory.
        """
        try:
            result = imfit(imagename=image_path, box=self.rec_aperture)
            comp = result['results']['component0']
            # Only write if flux is positive (to avoid bad fits)
            if comp['flux']['value'][0] > 0:
                outfile = os.path.join(os.getcwd(), outfile_base)
                with open(outfile, 'a') as f:
                    # The time stamp is extracted from the filename (depends on naming convention)
                    # Here we assume the time stamp is part of the directory name or image name.
                    # For simplicity, we write just the flux and error.
                    # In a real use you might want to extract the actual time.
                    f.write(f"{channel_id} {comp['flux']['value'][0]:2.3e} {comp['flux']['error'][0]:2.3e}\n")
                logging.info(f"Appended flux for {image_path} to {outfile}")
        except Exception as e:
            logging.warning(f"Flux extraction failed for {image_path}: {e}")

    # ----------------------------------------------------------------------
    # Public methods
    # ----------------------------------------------------------------------

    def slice_data(self) -> None:
        """
        Split the input visibility into time slices using CASA's split.
        For each time interval, a subdirectory is created and the sliced ms is placed there.
        """
        logging.info("Starting data slicing")
        # Compute number of intervals using integer arithmetic to avoid floating‑point issues
        total_duration = self.tim_end - self.tim_st
        # Ensure we don't go beyond the end time
        n_intervals = (total_duration - self.binsz) // self.step + 1

        # Initialise the first interval boundaries (as datetime objects)
        t1 = self.Tstart - datetime.timedelta(seconds=self.step)
        t2 = self.Tstart + datetime.timedelta(seconds=self.binsz - self.step)

        for _ in range(n_intervals):
            t1 += datetime.timedelta(seconds=self.step)
            t2 += datetime.timedelta(seconds=self.step)
            t_mid = t1 + (t2 - t1) / 2

            # Timerange string for CASA split
            timerange = f"{t1.time()}~{t2.time()}"

            # Output ms name (includes date and time)
            out_ms = f"{self.name}{t_mid.date()}T{t_mid.time()}_secs.ms"

            # Convert interval boundaries to seconds for directory name
            start_sec = self._to_seconds(t1)
            end_sec = self._to_seconds(t2)
            interval_dir = self._interval_dir(start_sec, end_sec)

            # Create the interval directory (if it doesn't exist)
            self._ensure_dir(interval_dir)

            # Full path for the output ms
            out_path = os.path.join(interval_dir, out_ms)

            # Run split only if the output doesn't already exist
            if os.path.exists(out_path):
                logging.info(f"Split output {out_path} already exists, skipping.")
            else:
                logging.info(f"Splitting {timerange} -> {out_path}")
                try:
                    split(vis=self.vis_file, outputvis=out_path,
                          datacolumn='data', timerange=timerange)
                except Exception as e:
                    logging.error(f"Split failed for interval {timerange}: {e}")
                    # Continue with next interval
        logging.info("Data slicing completed")

    def imaging(self, tim_st: Tuple[int, int]) -> None:
        """
        Run wsclean on the ms file(s) in the interval directory corresponding to (tim_st[0], tim_st[1]).

        Parameters
        ----------
        tim_st : tuple of two ints
            Start and end seconds (as used in directory name).
        """
        start_sec, end_sec = tim_st
        interval_dir = self._interval_dir(start_sec, end_sec)

        if not os.path.isdir(interval_dir):
            logging.warning(f"Interval directory {interval_dir} does not exist. Skipping.")
            return

        # Find all ms files in this interval directory
        ms_files = glob.glob(os.path.join(interval_dir, '*_secs.ms'))
        if not ms_files:
            logging.warning(f"No ms files found in {interval_dir}")
            return

        for ms_path in ms_files:
            ms_basename = os.path.basename(ms_path)
            # Check if imaging has already been done (by looking for the final MFS image)
            final_image = os.path.join(interval_dir, f'Image_{ms_basename}_final.fits')
            if os.path.exists(final_image):
                logging.info(f"Image {final_image} already exists, skipping wsclean for {ms_basename}")
                continue

            imgname = f'img_{ms_basename}'
            self._run_wsclean([ms_path], interval_dir, imgname, ms_basename)

    def extract_flux(self, tim_st: Tuple[int, int]) -> None:
        """
        Extract fluxes from all images in the interval directory.

        Parameters
        ----------
        tim_st : tuple of two ints
            Start and end seconds.
        """
        start_sec, end_sec = tim_st
        interval_dir = self._interval_dir(start_sec, end_sec)

        if not os.path.isdir(interval_dir):
            logging.warning(f"Interval directory {interval_dir} does not exist. Skipping flux extraction.")
            return

        # Iterate over all FITS files in the directory
        for filename in os.listdir(interval_dir):
            if not filename.endswith('.fits'):
                continue
            image_path = os.path.join(interval_dir, filename)

            # Determine the channel identifier and output file base
            # The naming convention is assumed:
            #   - MFS image: Image_<msname>_final.fits
            #   - Channel images: <anything>-<channel>-image.fits, e.g. ...-0000-image.fits
            if filename.endswith('_final.fits'):
                # MFS (full band)
                channel = 'MFS'
                outfile = f'lc_{self.name}{self.binsz}_secs_bin.txt'
            elif '-0000-image.fits' in filename:
                channel = '0000'
                outfile = f'lc_0000_{self.name}{self.binsz}_secs_bin.txt'
            elif '-0001-image.fits' in filename:
                channel = '0001'
                outfile = f'lc_0001_{self.name}{self.binsz}_secs_bin.txt'
            # Add more elifs as needed, or better: use a regex to extract channel number
            # For a generic approach, we could extract any pattern like -(\d{4})-image.fits
            else:
                # Try to match a four‑digit channel number
                import re
                match = re.search(r'-(\d{4})-image\.fits$', filename)
                if match:
                    channel = match.group(1)
                    outfile = f'lc_{channel}_{self.name}{self.binsz}_secs_bin.txt'
                else:
                    logging.debug(f"Skipping unrecognised FITS file: {filename}")
                    continue

            self._extract_flux_single(image_path, channel, outfile)

    def freq_and_flux(self, tim_st: Tuple[int, int]) -> None:
        """
        Extract frequency‑dependent flux (for spectrum) from images in the interval directory.
        Writes to a file named '<name>time_average_spectrum.txt'.
        """
        start_sec, end_sec = tim_st
        interval_dir = self._interval_dir(start_sec, end_sec)

        if not os.path.isdir(interval_dir):
            logging.warning(f"Interval directory {interval_dir} does not exist. Skipping.")
            return

        outfile = os.path.join(os.getcwd(), f'{self.name}time_average_spectrum.txt')

        for filename in os.listdir(interval_dir):
            if not (filename.endswith('-image.fits') or filename.endswith('_final.fits')):
                continue
            image_path = os.path.join(interval_dir, filename)

            try:
                result = imfit(imagename=image_path, box=self.rec_aperture)
                # Check that deconvolution gave a reasonable source (major axis > 1e-7)
                if result['deconvolved']['component0']['shape']['majoraxis']['value'] > 1e-7:
                    comp = result['results']['component0']
                    freq = comp['spectrum']['frequency']['m0']['value']
                    flux = comp['flux']['value'][0]
                    error = comp['flux']['error'][0]
                    with open(outfile, 'a') as f:
                        f.write(f"{freq:10f} {flux:2.3e} {error:2.3e}\n")
                    logging.info(f"Appended spectrum point from {filename}")
            except Exception as e:
                logging.warning(f"Could not extract spectrum from {filename}: {e}")

    def remove_crpt_image(self, tim_st: Tuple[int, int]) -> None:
        """
        Delete intermediate image files (e.g., per‑channel images, MFS images) in the interval directory.
        This is useful to clean up after successful extraction.
        """
        start_sec, end_sec = tim_st
        interval_dir = self._interval_dir(start_sec, end_sec)

        if not os.path.isdir(interval_dir):
            return

        for filename in os.listdir(interval_dir):
            if filename.endswith('-image.fits') or filename.endswith('_final.fits'):
                filepath = os.path.join(interval_dir, filename)
                os.remove(filepath)
                logging.info(f"Deleted {filepath}")from casa import imfit, split
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
                
                
                
                
                
                
                
                
                
                
                
                
                
                
                
                
                
import datetime
from Imaging_and_flux import ImagingAndFlux  # assuming you save the class in that file

Tstart = datetime.datetime(2020, 6, 16, 23, 45, 52)
Tstop = datetime.datetime(2020, 6, 16, 23, 59, 59)
binsz = 8
step = 8
name = 'ar_sco_'
vis = '1592350319_sdp_l0_1024ch_ARSco.ms'
basedir = '/idia/users/madzimest/arsco/arsco_2021_10/'
box = '1090,1090,1102,1102'
channels = '-channels-out 3 '
img_size = '-size 2192 2192 '

# Optional custom wsclean parameters
custom_wsclean = {
    '-niter': '60000',
    '-fit-spectral-pol': '4'
}

tr = ImagingAndFlux(t0=Tstart, t1=Tstop, binsz=binsz, step=step,
                    name=name, vis=vis, basedir=basedir,
                    rec_aperture=box, channelout=channels,
                    image_size=img_size, wsclean_params=custom_wsclean)

# Generate time intervals (same as before)
time_tr = []
for t in range(tr.tim_st, tr.tim_end - tr.binsz, tr.step):
    time_tr.append([t, t + tr.binsz])

# Run pipeline
tr.slice_data()

from multiprocessing.pool import ThreadPool as Pool
p = Pool(20)
p.map(tr.imaging, time_tr)
p.close()
p.join()

p = Pool(1)
p.map(tr.extract_flux, time_tr)
p.close()
p.join()                
