#!/bin/bash
#SBATCH --nodes=1            	# Numero de Nós
#SBATCH --ntasks-per-node=1  	# Numero de tarefas por Nó
#SBATCH --cpus-per-task=16   	# Numero de threads por tarefa
#SBATCH --partition=gpu      	# Fila (partition) a ser utilizada
#SBATCH --job-name=orchestra 	# Nome job
#SBATCH --exclusive         	# Utilização exclusiva dos nós durante a execução do job
#SBATCH --account=jodafons	# Contabilizar recursos

# Exibe os nós alocados para o job
echo $SLURM_JOB_NODELIST
nodeset -e $SLURM_JOB_NODELIST

module purge
module load cudnn/7.6.5 cuda/10.1

# Configura o numero de threads, de acordo com o parametro definido no Slurm
export OMP_NUM_THREADS=$SLURM_CPUS_PER_TASK

export IMG=/home/jodafons/public/images/orchestra_latest.sif

#srun -N 1 -n 1 -c $SLURM_CPUS_PER_TASK --gpus 2 singularity run --nv --writable-tmpfs $IMG pilot run --master &
srun -N 1 -n 1 -c $SLURM_CPUS_PER_TASK --gpus 2 singularity run --nv --writable-tmpfs $IMG pilot run &
#srun -N 1 -n 1 -c $SLURM_CPUS_PER_TASK --gpus 2 singularity run --nv --writable-tmpfs $IMG pilot run &
#srun -N 1 -n 1 -c $SLURM_CPUS_PER_TASK --gpus 2 singularity run --nv --writable-tmpfs $IMG pilot run &
#srun -N 1 -n 1 -c $SLURM_CPUS_PER_TASK --gpus 2 singularity run --nv --writable-tmpfs $IMG pilot run &

wait
