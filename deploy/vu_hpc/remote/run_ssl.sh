#!/bin/bash
#SBATCH -A alloc_52903_paraiska2026
#SBATCH -p gpu
#SBATCH --job-name=vietasr_ddp
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=7
#SBATCH --gres=gpu:7
#SBATCH --cpus-per-task=4
#SBATCH --mem=96G
#SBATCH --time=300:00:00
#SBATCH --chdir=/scratch/lustre/home/hpc_airenas/train-asr/VietASR

export MASTER_ADDR=$(scontrol show hostnames $SLURM_JOB_NODELIST | head -n 1)
export MASTER_PORT=12356

export WORLD_SIZE=$SLURM_NTASKS
export RANK=$SLURM_PROCID
export LOCAL_RANK=$SLURM_LOCALID

echo "MASTER_ADDR=$MASTER_ADDR"
echo "WORLD_SIZE=$WORLD_SIZE"
echo "RANK=$RANK"
echo "LOCAL_RANK=$LOCAL_RANK"

# srun singularity exec --nv \
#   /scratch/lustre/home/hpc_airenas/train-asr/containers/icefall_vu_hpc-0.1.sif \
#   make info gpus=$WORLD_SIZE multi_node=1 max_duration=1000 start_epoch=1 epoch_train=10

srun singularity exec --nv \
  /scratch/lustre/home/hpc_airenas/train-asr/containers/icefall_vu_hpc-0.1.sif \
  make gpus=$WORLD_SIZE pretrain multi_node=1 max_duration=1000 start_epoch=1 epoch_train=20
