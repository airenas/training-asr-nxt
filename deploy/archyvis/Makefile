-include Makefile.options
SSH_CMD=ssh -p $(port) -t $(user)@$(server)
RSYNC_CMD=rsync -e "ssh -p $(port)"
RSYNC_REMOTE_CMD=rsync -r -P
#########################################################################
ssh:
	ssh -p $(port) $(user)@$(server)

ssh/8787:
	ssh -L 8787:127.0.0.1:8787 -p $(port) $(user)@$(server)

ssh/psql:
	ssh -L 5432:127.0.0.1:5432 -p $(port) $(user)@$(server)

ssh/snipper:
	ssh -L 8000:127.0.0.1:8000 -p $(port) $(user)@$(server)

copy/scripts:
	$(SSH_CMD) "mkdir -p train-asr/bin"	
	scp -P $(port) -r ../../preparation  $(user)@$(server):~/train-asr/
	scp -P $(port) ../../Makefile  $(user)@$(server):~/train-asr/
	scp -P $(port) ../../requirements.txt  $(user)@$(server):~/train-asr/
	scp -P $(port) -r ./remote/*  $(user)@$(server):~/train-asr/
	scp -P $(port) -r ../../egs  $(user)@$(server):~/train-asr/
	scp -P $(port) ../../runner/target/release/zipper $(user)@$(server):~/train-asr/bin/
	scp -P $(port) ../../runner/target/release/db-filler $(user)@$(server):~/train-asr/bin/
	scp -P $(port) ../../runner/target/release/file-to-db $(user)@$(server):~/train-asr/bin/
	scp -P $(port) ../../runner/target/release/db-to-file $(user)@$(server):~/train-asr/bin/
	scp -P $(port) ../../runner/target/release/collect-jsonl $(user)@$(server):~/train-asr/bin/
	scp -P $(port) ../../runner/target/release/snipper-ws $(user)@$(server):~/train-asr/bin/
	scp -P $(port) ../../runner/target/release/runner $(user)@$(server):~/train-asr/bin/

#########################################################################
dwn/db: | ../../.backup/
	scp -r -P $(port) $(user)@$(server):~/train-asr/egs/db/backups/* ../../.backup/
.PHONY: dwn/db

#########################################################################
dwn/cluster: | ../../.backup/
	scp -r -P $(port) $(user)@$(server):~/train-asr/egs/speaker_embeddings/.data/clustered.jsonl ../../.backup/
.PHONY: dwn/cluster

../../.backup/:
	mkdir -p $@	
#########################################################################
