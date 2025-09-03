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
calc/speaker-times: 
	$(python_cmd) local/speaker_in_file.py
.PHONY: calc/speaker-times
############################################################
calc/speaker-lang:
	$(python_cmd) local/speaker_lang_in_file.py
.PHONY: calc/speaker-lang
############################################################
$(l_output_dir)/global_speaker.jsonl:
	$(python_cmd) local/global_speaker_durations.py --input ../../.backup/clustered.jsonl --output $(l_output_dir)/global_speaker.jsonl
calc/global-speaker: $(l_output_dir)/global_speaker.jsonl	
.PHONY: calc/global-speaker
############################################################
query: $(l_output_dir)/global_speaker.jsonl
	$(python_cmd) local/query_global.py --input $(l_output_dir)/global_speaker.jsonl
.PHONY: query
############################################################
clean: 
	rm -f $(l_output_dir)/speaker_out.json
.EXPORT_ALL_VARIABLES:
