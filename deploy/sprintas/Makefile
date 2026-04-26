-include Makefile.options
SSH_CMD=ssh -p $(port) -t $(user)@$(server)
RSYNC_CMD=rsync -e "ssh -p $(port)" -avz
viet_asr_dir=../../../VietASR
#########################################################################
ssh:
	ssh -p $(port) $(user)@$(server)

copy/scripts:
	$(RSYNC_CMD) -r --exclude __pycache__ ../../preparation  $(user)@$(server):~/train-asr/
	$(RSYNC_CMD) ../../Makefile  $(user)@$(server):~/train-asr/
	$(RSYNC_CMD) ../../requirements.txt  $(user)@$(server):~/train-asr/
	
	$(RSYNC_CMD) -r ./remote/*  $(user)@$(server):~/train-asr/
	$(RSYNC_CMD) --exclude .data --exclude __pycache__ ../../egs  $(user)@$(server):~/train-asr/
.PHONY: copy/scripts

copy/viet-asr:
	$(RSYNC_CMD) -r $(viet_asr_dir)/ $(user)@$(server):~/VietASR/
	$(RSYNC_CMD) -r ./remote_vietasr/*  $(user)@$(server):~/VietASR
.PHONY: copy/viet-asr
#########################################################################
