############################################################
-include ../../Makefile.options
############################################################
corpus_dir?=./test
output_dir?=./.data
l_output_dir?=./.data
snipper-ws?=../../runner/target/release/snipper-ws
workers?=4
RUST_LOG?=trace
log?=INFO
python_cmd=PYTHONPATH=./../../ LOG_LEVEL=$(log) python
skip_input?=0
sources?=--source liepa3-speaker
dry_run?=1
############################################################
$(output_dir) $(l_output_dir):
	mkdir -p $@
	
############################################################
run/snipper-ws: 
	$(snipper-ws) --input-base $(output_dir)
.PHONY: run/snipper-ws
############################################################
$(l_output_dir)/speaker_out.json : $(l_output_dir)/speaker.json
	$(python_cmd) local/speaker.py --input $^ --output $@
prepare/speaker-json: $(l_output_dir)/speaker_out.json
.PHONY: prepare/speaker-json

$(l_output_dir)/speaker.wav : $(l_output_dir)/speaker_out.json
	curl -f -X POST http://localhost:8000/concat -H "Content-Type: application/json" --data-binary @$^ --output $@_
	mv $@_ $@

prepare/speaker-wav: $(l_output_dir)/speaker.wav
.PHONY: prepare/speaker-wav
############################################################
insert/speaker/times:
	$(python_cmd) local/speaker_in_file.py $(sources)
.PHONY: insert/speaker/times
############################################################
update/speaker/lang:
	$(python_cmd) local/speaker_lang_in_file.py $(sources)
.PHONY: update/speaker/lang
############################################################
update/speaker/cluster:
	$(python_cmd) local/speaker_cluster_in_file.py --clusters $(clusters)
.PHONY: update/speaker/cluster
############################################################
update/speaker/gender:
	$(python_cmd) local/speaker_gender_in_file.py --clusters $(clusters) --genders $(genders)
.PHONY: update/speaker/gender
############################################################
$(l_output_dir)/global_speaker.jsonl:
	$(python_cmd) local/global_speaker_durations.py --input ../speaker_embeddings/.data/clustered.jsonl --output $(l_output_dir)/global_speaker.jsonl
calc/global-speaker: $(l_output_dir)/global_speaker.jsonl	
.PHONY: calc/global-speaker
############################################################
query: $(l_output_dir)/global_speaker.jsonl
	$(python_cmd) local/query_global.py --input $(l_output_dir)/global_speaker.jsonl
.PHONY: query
############################################################
run/label/gender: | $(l_output_dir)
	$(python_cmd) local/label_gender.py --input ../speaker_embeddings/.data/clustered.jsonl --output $(l_output_dir)/gender.jsonl \
	--model ../gender/.data/model.json \
	--audio-url http://localhost:8000/concat --skip-input $(skip_input)
############################################################
run/cluster/analysis: 
	$(python_cmd) local/cluster_analysis.py --input ../speaker_embeddings/.data/clustered.jsonl \
	--audio-url http://localhost:8000/concat --skip-input $(skip_input)
############################################################
run/speaker/analysis:
	$(python_cmd) local/speaker_analysis.py \
	--audio-url http://localhost:8000/concat
############################################################
prepare/speaker-rttm:
	$(python_cmd) local/prepare_speaker_rttm.py --dry-run $(dry_run) --output .data/audio.segments.jsonl
calc/speaker-selection:
	$(python_cmd) local/calc_speaker_selection.py --input .data/audio.segments.jsonl	 --dry-run $(dry_run) --output .data/final.segments.jsonl	
############################################################
clean: 
	rm -f $(l_output_dir)/speaker_out.json
.EXPORT_ALL_VARIABLES:
