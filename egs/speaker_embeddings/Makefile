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
minimum_memory?=5GB
PYTHONPATH=./../../
LOG_LEVEL=$(log)
RUST_LOG?=trace
############################################################
$(output_dir) $(cache_dir):
	mkdir -p $@
	
run/runner: | $(output_dir)  $(cache_dir)
	$(runner) --input $(output_dir) --output $(output_dir) --workers $(workers) \
		--cmd "python local/embeddings.py --input {input} --output {output}" \
		--output-file speaker.embeddings.jsonl --extensions audio.rttm --minimum-memory $(minimum_memory) \
		--same-dir --cache-file $(cache_dir)/.embeddings.cache
.PHONY: run/runner	

.data:
	mkdir -p $@

.data/all.embeddings.jsonl:  | .data
	$(python_cmd) local/collect_all.py  --input $(output_dir) --output $@

collect: .data/all.embeddings.jsonl
.PHONY: collect

.data/clustered.jsonl: .data/all.embeddings.jsonl | .data
	$(python_cmd) local/cluster.py --input .data/all.embeddings.jsonl --output $@
cluster: .data/clustered.jsonl
.PHONY: cluster

.EXPORT_ALL_VARIABLES:
