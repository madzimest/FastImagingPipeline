# Fast Imaging Pipeline

This repository provides a pipeline to perform fast radio imaging and flux extraction from Measurement Set (MS) files using CASA and WSClean.

## Features
- Slice MS files into time intervals using CASA `split`.
- Run WSClean imaging on each interval.
- Extract flux from images using CASA `imfit`.
- Multiprocessing support to speed up computations.

## Requirements
- [CASA](https://casa.nrao.edu/)
- [WSClean](https://sourceforge.net/p/wsclean/wiki/Home/)
- Python 3.x
- Standard Python packages: `multiprocessing`, `glob`, `os`, `shutil`, `datetime`

## Installation
Clone this repository:
```bash
git clone https://github.com/<your_username>/FastImagingPipeline.git
cd FastImagingPipeline
