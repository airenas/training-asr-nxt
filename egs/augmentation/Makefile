############################################################
-include ../../Makefile.options
############################################################
log?=INFO
python_cmd=PYTHONPATH=./../../ LOG_LEVEL=$(log) python
aug_dir?=./aug-rir-noise
basedir?=/home/airenas/projects/training-asr-nxt/egs/augmentation/aug-rir-noise
workers?=10
########################################################################################################
## print usage information
help:
	@echo 'Usage:'
	@cat ${MAKEFILE_LIST} | grep -e "^## " -A 1 | grep -v '\-\-' | sed 's/^##//' | cut -f1 -d":" | \
		awk '{info=$$0; getline; print "  " $$0 ": " info;}' | column -t -s ':' | sort
########################################################################################################
$(aug_dir)/data_in/wav.scp: 
	$(python_cmd) local/make_wav_list.py --dir $(aug_dir)/data_in --out $(aug_dir)/data_in/wav.scp
wav-scp: $(aug_dir)/data_in/wav.scp
.PHONY: wav-scp

$(aug_dir)/aug_instructions.txt: $(aug_dir)/data_in/wav.scp
	cd $(aug_dir) && instructions=true ./gen_and_augment.sh
instructions: $(aug_dir)/aug_instructions.txt
.PHONY: instructions

augmentation: $(aug_dir)/aug_instructions.txt
	cd $(aug_dir) && augmentation=true ./gen_and_augment.sh
.PHONY: augmentation

run/s3-list-augmentation: 
	$(python_cmd) local/s3_list_augmentation.py --aug-dir $(aug_dir) \
		--s3-bucket audio-corpus/crawl --s3-from 0 --s3-to 5 --s3-bucket-output audio-corpus/crawl-augmented \
		--workers $(workers)
.PHONY: augmentation
########################################################################################################
clean:
	rm -rf $(aug_dir)/data_in/wav.scp
########################################################################################################
.EXPORT_ALL_VARIABLES:
