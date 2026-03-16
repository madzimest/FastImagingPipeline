import datetime
from multiprocessing import Pool
import sys
sys.path.append('/scratch3/users/madzimest/arsco/')
from Fast_imaging import Imaging_and_flux


# Inputs
Tstart = datetime.datetime(2020,06,16,23,45,52) #starting time should be in this format
Tstop = datetime.datetime(2020,06,16,23,59,59)  #End time
binsz_in_seconds = 8
step_size = 8
input_vis = '1592350319_sdp_l0_1024ch_ARSco.ms' # give the name of calibrated ms file, it can be in any folder but provide path
name = 'ar_sco_' # provide the name of your source
dir = '/idia/users/madzimest/arsco/arsco_2021_10/' # provide the path where you want to store your outputs to avoid transferring after the analysis i.e., /idia/users/<username>/<storage_directory>
box_aperture = '1090,1090,1102,1102' # [x-btc,y-btc,x-tpc,y-tpc],btc=bottom and corner tpc=top corner. Best coordinates from full range wsclean fits file
num_channels = '-channels-out 3 ' # specify number of channels, you slice the frequency band into small bands useful to calculate the index and time average spectrum
img_size = '-size 2192 2192 '   #specify the size of the output image, must be small to run multiple process without any interruption

tr = Imaging_and_flux(t0 = Tstart, t1 = Tstop, binsz = binsz_in_seconds, step = step_size, name = name, vis = input_vis, basedir = dir, rec_aperture = box_aperture, channelout = num_channels, image_size = img_size)
#p = concurrent.futures.ThreadPoolExecutor()
p = Pool(20)
#time_tr = np.arange(tr.tim_st, tr.tim_end, tr.binsz)

time_tr = []
for t_bin in range(tr.tim_st, tr.tim_end-tr.binsz, tr.step):
    try:
        time_0 = t_bin
        time_1 = tr.binsz + t_bin
        time_tr.append([time_0, time_1])
    except Exception:
        problem = "end"


tr.slice_data()
#p.map(tr.remove_crpt_image, time_tr) # this will delete generated fits image files files in each folder.
p.map(tr.imaging, time_tr) #imaging
p = Pool(1)
#p.map(tr.Freq_and_flux,time_tr) # extract flux as a function of time, produce a txt file (frequency, flux, fluxerror)
p.map(tr.Extract_flux,time_tr) # extract flux and time, produce a txt file (date, flux, fluxerror)
