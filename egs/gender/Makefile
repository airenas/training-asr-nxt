############################################################
-include ../../Makefile.options
log?=INFO
python_cmd=PYTHONPATH=./../../ LOG_LEVEL=$(log) python
cluster_file?=../speaker_embeddings/.data/clustered.jsonl
############################################################
l_output_dir?=./.data
gender_model?=$(l_output_dir)/model.json
############################################################
$(output_dir) $(l_output_dir):
	mkdir -p $@
############################################################
$(l_output_dir)/gender_train.jsonl : | $(l_output_dir)
	$(python_cmd) local/prepare_train.py --input-cluster ../speaker_embeddings/.data/clustered.jsonl \
	        --input-gender ../analysis/.data/gender.jsonl --output $@_
	mv $@_ $@
prepare/train: $(l_output_dir)/gender_train.jsonl
.PHONY: prepare/train	
############################################################
$(l_output_dir)/model.json: $(l_output_dir)/gender_train.jsonl | $(l_output_dir)
	$(python_cmd) local/train.py --input $< --output $@
train: $(l_output_dir)/model.json
.PHONY: train
############################################################
$(l_output_dir)/%-gender.jsonl: $(l_output_dir)/%-clusters.jsonl | $(l_output_dir)
	$(python_cmd) local/detect.py --input-cluster $(l_output_dir)/$*-clusters.jsonl --output $@_ \
	--model $(gender_model)
	mv $@_ $@
detect/gender/liepa3-speaker: $(l_output_dir)/liepa3-speaker-gender.jsonl
.PHONY: detect/gender/liepa3-speaker
detect/gender/all: $(l_output_dir)/all-gender.jsonl
.PHONY: detect/gender/all
############################################################
clean:
	rm -rf $(l_output_dir)
.EXPORT_ALL_VARIABLES:
