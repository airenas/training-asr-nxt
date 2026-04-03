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
$(l_output_dir)/audio.segments.jsonl:
	$(python_cmd) local/prepare_speaker_rttm.py --dry-run $(dry_run) --output $(l_output_dir)/audio.segments.jsonl_
	mv $@_ $@
prepare/speaker-rttm: $(l_output_dir)/audio.segments.jsonl
.PHONY: prepare/speaker-rttm	
############################################################
take_up?=3600
take_up_crawl_male?=3000	
take_up_crawl_female?=3600
$(l_output_dir)/final.segments.jsonl: $(l_output_dir)/audio.segments.jsonl
	$(python_cmd) local/calc_speaker_selection.py --input  $(l_output_dir)/audio.segments.jsonl --dry-run $(dry_run) --output  $(l_output_dir)/final.segments.jsonl_ \
		--take-up $(take_up) --take-up-crawl-male $(take_up_crawl_male) --take-up-crawl-female $(take_up_crawl_female)
	mv $@_ $@
calc/speaker-selection: $(l_output_dir)/final.segments.jsonl
.PHONY: calc/speaker-selection		
############################################################
$(l_output_dir)/file.cuts.jsonl: $(l_output_dir)/final.segments.jsonl
	$(python_cmd) local/calc_file_cuts.py --input $(l_output_dir)/final.segments.jsonl --dry-run $(dry_run) --output $@_
	mv $@_ $@
calc/file-cuts: $(l_output_dir)/file.cuts.jsonl
.PHONY: calc/file-cuts
############################################################
clean: 
	rm -f $(l_output_dir)/speaker_out.json
.EXPORT_ALL_VARIABLES:
	