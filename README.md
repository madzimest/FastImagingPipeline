# Fast Imaging Pipeline for Radio Interferometry Light Curves

This pipeline processes a calibrated CASA measurement set to produce high‑time‑resolution light curves and (optionally) a time‑averaged spectrum. It splits the data into short time intervals, images each interval with `wsclean`, and extracts flux densities using CASA’s `imfit`. The pipeline is designed to run on a SLURM cluster using a Singularity container that provides CASA and wsclean, but can also be executed interactively if the software is installed locally.



## Table of Contents
- [Overview](#overview)
- [Requirements](#requirements)
- [File Structure](#file-structure)
- [Configuration Parameters](#configuration-parameters)
- [How to Run](#how-to-run)
  - [Interactive (testing)](#interactive-testing)
  - [Batch (SLURM)](#batch-slurm)


## Overview

The pipeline performs the following steps for a given observation:

1. Slicing, The measurement set is divided into overlapping or consecutive time bins using CASA’s `split`. Each bin is saved in a separate subdirectory.
2. Imaging, For each time bin, `wsclean` is run to create images (both full‑band and per‑channel, depending on the `-channels-out` setting).
3. Flux extraction, CASA’s `imfit` measures the flux density of the target in each image. Results are written to text files:
   - Light curve: time vs. flux (and error)
   - Spectrum: frequency vs. flux (if requested)
4. Optional cleanup, Intermediate FITS files can be removed to save space.

The pipeline is parallelised using Python’s `multiprocessing` pool, and the SLURM batch script allocates the necessary resources.



## Requirements

### Software
- CASA (version 5.6 or later recommended), used for `split` and `imfit`.
- wsclean, the imager (must be in the `PATH`).
- Python 3, standard libraries only (`datetime`, `os`, `glob`, `shutil`, `multiprocessing`).

### Hardware / Cluster
- The pipeline is intended for a SLURM‑managed cluster.
- A typical job uses 32 CPUs and 230 GB of RAM (adjust according to your data size and image parameters).

### Container
The provided batch script uses a Singularity image (`kern5.simg`) that contains both CASA and wsclean. If you are on a different system, you can either:
- Build a similar container, or
- Install CASA and wsclean natively and run the Python driver directly (see [Interactive run](#interactive-testing)).



## File Structure

 File  Description 
-
 `Fast_imaging.py`  Contains the `ImagingAndFlux` class with methods for slicing, imaging, flux extraction, and cleanup. 
 `run_Fast_imaging.py`  Python driver: defines observation parameters, generates time intervals, and calls the class methods in parallel. 
 `go_run_Fast_imaging.py`  SLURM batch script that loads the Singularity container and executes `run_Fast_imaging.py`. 



## Configuration Parameters

All adjustable parameters are located at the top of `run_Fast_imaging.py`. Below is a detailed explanation of each.

 Parameter  Type  Description 


 `Tstart`, `Tstop`  `datetime.datetime`  Start and end time of the observation (format: `YYYY,MM,DD,HH,MM,SS`). 
 `binsz_in_seconds`  int  Length of each time bin (integration time) in seconds. 
 `step_size`  int  Step between consecutive bins in seconds. If `step < binsz`, bins overlap. 
 `input_vis`  str  Path to the calibrated CASA measurement set (`.ms`). 
 `name`  str  Source name prefix used for output files and directory names. 
 `dir`  str  Base directory where all output subdirectories will be created. 
 `box_aperture`  str  Pixel coordinates for the box used by `imfit`, given as `"x1,y1,x2,y2"`. These must be determined from a full‑time image of the source. 
 `num_channels`  str  `wsclean` parameter, e.g., `"-channels-out 3 "`. The number of frequency channels to image. 
 `img_size`  str  Image size in pixels, e.g., `"-size 2192 2192 "`. 

Important: The `box_aperture` must be chosen carefully. Open a full‑time image of your target in a viewer (e.g., CARTA, ds9) and note the pixel coordinates of a box that encloses the source. These coordinates depend on the image size; if you change `img_size`, you may need to recompute them.



## How to Run

### Interactive (testing)

If you have CASA and wsclean installed natively (or inside a container you can enter interactively), you can run the pipeline directly:

```bash
casa --nogui --nologger -c run_Fast_imaging.py
