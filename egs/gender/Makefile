############################################################
-include ../../Makefile.options
log?=INFO
python_cmd=PYTHONPATH=./../../ LOG_LEVEL=$(log) python
cluster_file?=../speaker_embeddings/.data/clustered.jsonl
gender_model?=$(l_output_dir)/model.json
############################################################
l_output_dir?=./.data
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
$(l_output_dir)/cluster_gender.jsonl: | $(output_dir)
	$(python_cmd) local/detect.py --input-cluster $(cluster_file) --output $@_ \
	--model $(gender_model)
	mv $@_ $@
run/detect/gender: $(l_output_dir)/cluster_gender.jsonl
.PHONY: run/detect/gender
############################################################
clean:
	rm -rf $(l_output_dir)
.EXPORT_ALL_VARIABLES:
