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
	scp -P $(port) ../../runner/target/release/runner $(user)@$(server):~/train-asr/bin/

dwn/ina: | ../../.backup/
	scp -P $(port) $(user)@$(server):~/train-asr/egs/sm_detection_ina/ina_segments.zip ../../.backup/
.PHONY: dwn/ina

dwn/rttm: | ../../.backup/
	scp -P $(port) $(user)@$(server):~/train-asr/egs/diarization/rttm.zip ../../.backup/rttm-$$(date +%Y%m%d_%H%M%S).zip
.PHONY: dwn/rttm

dwn/speaker.embeddings: | ../../.backup/
	scp -P $(port) $(user)@$(server):~/train-asr/egs/speaker_embeddings/speaker.embeddings.zip ../../.backup/
.PHONY: dwn/speaker.embeddings

dwn/lang: | ../../.backup/
	scp -P $(port) $(user)@$(server):~/train-asr/egs/id_lang/lang.zip ../../.backup/
.PHONY: dwn/lang

../../.backup/:
	mkdir -p $@	
