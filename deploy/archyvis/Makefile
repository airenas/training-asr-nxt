-include Makefile.options
SSH_CMD=ssh -p $(port) -t $(user)@$(server)
RSYNC_CMD=rsync -e "ssh -p $(port)"
RSYNC_REMOTE_CMD=rsync -r -P
#########################################################################
ssh:
	ssh -p $(port) $(user)@$(server)

ssh/8787:
	ssh -L 8787:127.0.0.1:8787 -p $(port) $(user)@$(server)

copy/scripts:
	$(SSH_CMD) "mkdir -p train-asr"	
	scp -P $(port) -r ../../preparation  $(user)@$(server):~/train-asr/
	scp -P $(port) ../../Makefile  $(user)@$(server):~/train-asr/
	scp -P $(port) ../../requirements.txt  $(user)@$(server):~/train-asr/
	scp -P $(port) -r ./remote/*  $(user)@$(server):~/train-asr/
