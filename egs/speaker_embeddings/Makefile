############################################################
-include ../../Makefile.options
############################################################
log?=INFO
python_cmd=PYTHONPATH=./../../ LOG_LEVEL=$(log) python
# CUDA?=cuda:0
# CUDA_VISIBLE_DEVICES=
TF_CPP_MIN_LOG_LEVEL=3
corpus_dir?=./test
output_dir?=./.data
cache_dir=./../../.cache
workers?=4
memory_limit?=0
limit_mb?=50
runner?=../../runner/target/release/runner
collector?=../../runner/target/release/colect-jsonl
runner-wrk?=../../runner/target/release/runner-wrk
minimum_memory?=5GB
PYTHONPATH=./../../
LOG_LEVEL=$(log)
RUST_LOG?=trace
############################################################
$(output_dir) $(cache_dir):
	mkdir -p $@
	
run/runner: | $(output_dir)  $(cache_dir)
	$(runner) --input-base $(output_dir) --workers $(workers) \
		--cmd "python local/embeddings.py --input_wav {input_wav} --input_rttm {input} --output {output}" \
		--output-files=speaker.embeddings.jsonl --input-files=audio.rttm --minimum-memory $(minimum_memory)
.PHONY: run/runner

run/runner-wrk: | $(output_dir) 
	$(runner-wrk) --input-base $(output_dir) --workers $(workers) \
		--cmd "python local/wrk_embeddings.py" \
		--output-files=speaker.embeddings.jsonl --minimum-memory $(minimum_memory) --source $(source) \
		--input-files=audio.rttm
.PHONY: run/runner-wrk

run/runner-wrk/on-same-wav: 
	$(runner-wrk) --input-base "" --workers $(workers) \
		--cmd "python local/wrk_embeddings.py" \
		--output-files=speaker.embeddings.jsonl --minimum-memory $(minimum_memory) --source $(source) \
		--input-files=audio.rttm
.PHONY: run/runner-wrk

.data:
	mkdir -p $@

.data/emb-%.jsonl:  | .data
	$(collector) --name speaker.embeddings.jsonl --output $@_ --source $*
	mv $@_ $@

.data/emb-all.jsonl: .data/emb-08kHz.jsonl .data/emb-16kHz.jsonl .data/emb-crawl.jsonl .data/emb-voxlingua.jsonl .data/emb-voxpopuli.jsonl | .data
	cat $^ > $@
collect/liepa3-speaker: .data/emb-liepa3-speaker.jsonl
.PHONY: collect/liepa3-speaker

collect: .data/emb-all.jsonl
.PHONY: collect

.data/clustered.jsonl: .data/emb-all.jsonl | .data
	$(python_cmd) local/cluster.py --input .data/emb-all.jsonl --output $@
cluster: .data/clustered.jsonl
.PHONY: cluster

.EXPORT_ALL_VARIABLES:
