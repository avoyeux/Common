#!/bin/bash

# Job name
#SBATCH --job-name Rainbow

# Partition to use
#SBATCH --partition all
##SBATCH --exclusive

# Resources
##SBATCH --nodelist cluster-r7920-1,cluster-r7920-2,cluster-r730-k80-1
#SBATCH --exclude cluster-r730-k80-1,cluster-r740-volta-1
#SBATCH --nodes 1          # Number of nodes
#SBATCH --mem 10Gb         # Memory
#SBATCH --ntasks-per-node 4 

# Work directory set as file directory
SCRIPT_DIR=$(dirname "$(realpath "$0")")
#SBATCH --chdir="$SCRIPT_DIR"

echo "Starting at `date`"
echo "job name is $SLURM_JOB_NAME"
echo "Running on hosts: $SLURM_NODELIST"
echo "Running on $SLURM_NNODES nodes."
echo "Running on $SLURM_NPROCS processors."
echo "Task per node $SLURM_TASKS_PER_NODES"
echo "job cpu per node = $SLURM_JOB_CPUS_PER_NODE"
echo "Current working directory is `pwd`"

source /home/avoyeux/.bashrc
source ../.venv_dev/bin/activate
python3 --version
python3 filename.py
