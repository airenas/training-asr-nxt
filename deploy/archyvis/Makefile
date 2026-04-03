-include Makefile.options
SSH_CMD=ssh -p $(port) -t $(user)@$(server)
RSYNC_CMD=rsync -e "ssh -p $(port)" -avz
RSYNC_REMOTE_CMD=rsync -r -P
#########################################################################
ssh:
	ssh -p $(port) $(user)@$(server)

ssh/8787:
	ssh -L 8787:127.0.0.1:8787 -p $(port) $(user)@$(server)

ssh/psql:
	ssh -L 5432:127.0.0.1:5432 -L 8000:127.0.0.1:8000 -p $(port) $(user)@$(server)

ssh/snipper:
	ssh -L 8000:127.0.0.1:8000 -p $(port) $(user)@$(server)

copy/scripts:
	$(SSH_CMD) "mkdir -p train-asr/bin"	
	$(RSYNC_CMD) -r ../../preparation  $(user)@$(server):~/train-asr/
	scp -P $(port) ../../Makefile  $(user)@$(server):~/train-asr/
	scp -P $(port) ../../requirements.txt  $(user)@$(server):~/train-asr/
	$(RSYNC_CMD) -r ./remote/*  $(user)@$(server):~/train-asr/
	$(RSYNC_CMD) --exclude .data ../../egs  $(user)@$(server):~/train-asr/
	scp -P $(port) ../../runner/target/release/zipper $(user)@$(server):~/train-asr/bin/
	scp -P $(port) ../../runner/target/release/db-filler $(user)@$(server):~/train-asr/bin/
	scp -P $(port) ../../runner/target/release/file-to-db $(user)@$(server):~/train-asr/bin/
	scp -P $(port) ../../runner/target/release/db-to-file $(user)@$(server):~/train-asr/bin/
	$(RSYNC_CMD) ../../runner/target/release/collect-jsonl $(user)@$(server):~/train-asr/bin/
	$(RSYNC_CMD) ../../runner/target/release/snipper-ws $(user)@$(server):~/train-asr/bin/
	scp -P $(port) ../../runner/target/release/runner-wrk $(user)@$(server):~/train-asr/bin/
	scp -P $(port) ../../runner/target/release/runner $(user)@$(server):~/train-asr/bin/
	scp -P $(port) ../../runner/target/release/fs-runner $(user)@$(server):~/train-asr/bin/
	$(RSYNC_CMD) ../../runner/target/release/vad $(user)@$(server):~/train-asr/bin/
	$(RSYNC_CMD) $(onnx_lib)/* $(user)@$(server):~/train-asr/bin/onnx/

#########################################################################
dwn/db: | ../../.backup/
	scp -r -P $(port) $(user)@$(server):~/train-asr/egs/db/backups/* ../../.backup/
.PHONY: dwn/db

#########################################################################
dwn/cluster: | ../../.backup/
	scp -r -P $(port) $(user)@$(server):~/train-asr/egs/speaker_embeddings/.data/clustered.jsonl ../../.backup/
.PHONY: dwn/cluster

../../.backup/ .data:
	mkdir -p $@	
#########################################################################
dwn/sample: | .data
# 	scp -r -P $(port) $(user)@$(server):/mnt/md1/audio-corpus/16khz/vu-liepa1/WAV/D118/Z022/Z118Mi_022_104_02.wav/audio.16.wav .data/
	scp -r -P $(port) $(user)@$(server):/mnt/md1/aireno/voxpopuli/v1/raw_audios/lt/2017/20171212-0900-PLENARY-23_lt.ogg .data/
.PHONY: dwn/sample
