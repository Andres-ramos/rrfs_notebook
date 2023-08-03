#!/bin/sh
#BATCH -A wrfruc
#SBATCH -t 8:00:00
#SBATCH --nodes=1 --ntasks=1
#SBATCH --partition=service

module purge
module use -a /apps/contrib/miniconda3-noaa-gsl/modulefiles
module load miniconda3
conda activate /work2/noaa/wrfruc/aramos/envs
python download_data.py

