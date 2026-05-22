-include Makefile.options
###########################################################################
allocation?=
container?=containers/icefall_vu_hpc-0.1.sif
gpus?=4
cpus?=16
mem?=64G
time?=24:00:00
# gpu, gpu2
machine?=gpu
time?=24:00:00
work_dir?=~/train-asr/VietASR
###########################################################################
info: jobs/queue
	@echo "------------------------------------------"
	@echo "gpus: $(gpus)"
	@echo "cpus: $(cpus)"
	@echo "mem: $(mem)"
	@echo "time: $(time)"
	@echo "machine: $(machine)"
	@echo "container: $(container)"
	@echo "allocation: $(allocation)"
	@echo "work_dir: $(work_dir)"
.PHONY: info	
###########################################################################
jobs/info:
	sinfo ${allocation}

jobs/queue:
	squeue ${allocation}
.PHONY: jobs/queue
###########################################################################
run/local/shell:
	singularity shell --nv $(container)
.PHONY: run/local/shell	

run/gpu:
	srun $(allocation) -p $(machine) --gres gpu:$(gpus) --cpus-per-task=$(cpus) \
	--mem=$(mem) --pty $$SHELL
.PHONY: run/local/gpu

run/gpu/info:
	srun --jobid=$(job_id) --overlap --export ALL $$SHELL -c 'nvidia-smi'
.PHONY: run/gpu/info
run/cpu/info:
	@echo "CPU info for job $(job_id):"
	@echo "Warning terminal is broken afer this command"
	@Pause for 5 seconds to read the output...
	sleep 5
	srun --jobid=$(job_id) --overlap --export ALL $$SHELL -c 'htop'	
.PHONY: run/cpu/info

run/train:
	sbatch $(allocation) -p $(machine) \
		--gres gpu:$(gpus) \
		--cpus-per-task=$(cpus) \
		--mem=$(mem) \
		--time=$(time) \
		--chdir=$(work_dir) \
		--job-name=train_$(gpus)gpu \
		--wrap="singularity exec --nv $(container) make gpus=$(gpus) train max_durations=8000"
###########################################################################
job/describe: 
	scontrol show job $(job_id)
.PHONY: job/describe
job/stop:
	scancel $(job_id)
.PHONY: job/stop
