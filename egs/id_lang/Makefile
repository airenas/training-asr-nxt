############################################################
-include ../../Makefile.options
############################################################
log?=INFO
python_cmd=PYTHONPATH=./../../ LOG_LEVEL=$(log) python
# CUDA?=cuda:0
CUDA_VISIBLE_DEVICES=
TF_CPP_MIN_LOG_LEVEL=3
corpus_dir?=./test
output_dir?=./.data
cache_dir=./../../.cache
workers?=4
memory_limit?=0
limit_mb?=50
runner?=../../runner/target/release/runner
minimum_memory?=10GB
PYTHONPATH=./../../
LOG_LEVEL=$(log)
RUST_LOG?=trace

############################################################
$(output_dir) $(cache_dir):
	mkdir -p $@
	
run: | $(output_dir)
# 	$(python_cmd) local/run.py --input $(corpus_dir) --output $(output_dir) --workers $(workers) --memory_limit $(memory_limit) \
# 	    --limit_mb $(limit_mb)
	$(python_cmd) local/id.py  --input $(corpus_dir) --output $(output_dir)
.PHONY: run	

run/runner: | $(output_dir)  $(cache_dir)
	$(runner) --input $(output_dir) --output $(output_dir) --workers $(workers) \
		--cmd "python local/id.py --input {input} --output {output}" \
		--output-file lang_detect.jsonl --extensions wav --minimum-memory $(minimum_memory) \
		--same-dir --cache-file $(cache_dir)/.language.cache
.PHONY: run/runner	

.EXPORT_ALL_VARIABLES:
