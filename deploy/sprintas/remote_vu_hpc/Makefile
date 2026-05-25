-include Makefile.options
RSYNC_CMD=rsync -e "ssh -p $(port)" -avz --progress
data_dir?=/mnt/42T/experiments/VietASR

#########################################################################
ssh:
	ssh -p $(port) $(user)@$(server)

copy/lang:
	$(RSYNC_CMD) $(data_dir)/lang_bpe_500 $(user)@$(server):~/train-asr/experiments/VietASR/
.PHONY: copy/lang

copy/manifests:
	$(RSYNC_CMD) $(data_dir)/manifests $(user)@$(server):~/train-asr/experiments/VietASR/
.PHONY: copy/manifests

copy/fbank:
	$(RSYNC_CMD) $(data_dir)/fbank $(user)@$(server):~/train-asr/experiments/VietASR/
.PHONY: copy/fbank

copy/fbank-fixed:
	$(RSYNC_CMD) $(data_dir)/fbank-fixed $(user)@$(server):~/train-asr/experiments/VietASR/
.PHONY: copy/fbank-fixed
