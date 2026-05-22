-include Makefile.options
RSYNC_CMD=rsync -e "ssh -p $(port)" -avz --progress
viet_asr_dir=../../../VietASR


#########################################################################
ssh:
	ssh -p $(port) $(user)@$(server)

copy/scripts:
	$(RSYNC_CMD) ./remote/*  $(user)@$(server):~/train-asr/
.PHONY: copy/scripts

copy/apptainer:
	$(RSYNC_CMD) ./docker/icefall_vu_hpc-0.1.sif  $(user)@$(server):~/train-asr/containers/
	
copy/viet-asr:
	$(RSYNC_CMD) --exclude='Makefile.options' \
		--exclude='data/' \
		--exclude='corpus/'\
		--exclude='.idea/'\
		--exclude='.git/'\
		  $(viet_asr_dir)/ $(user)@$(server):~/train-asr/VietASR/
	$(RSYNC_CMD) ./remote_vietasr/*  $(user)@$(server):~/train-asr/VietASR/
.PHONY: copy/viet-asr
#########################################################################
