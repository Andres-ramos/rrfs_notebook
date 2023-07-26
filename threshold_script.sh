#!/bin/sh
#SBATCH -A wrfruc
#SBATCH -t 2:00:00
#SBATCH --nodes=1 --ntasks=1
#SBATCH --partition=orion
#SBATCH -o ./slurm/

module purge
module use -a /apps/contrib/miniconda3-noaa-gsl/modulefiles
module load miniconda3
conda activate /work2/noaa/wrfruc/aramos/envs
python threshold.py $1
