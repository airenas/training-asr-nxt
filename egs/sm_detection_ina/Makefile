############################################################
-include ../../Makefile.options
############################################################
log?=INFO
PYTHONPATH=./../../
LOG_LEVEL=$(log)
python_cmd=PYTHONPATH=./../../ LOG_LEVEL=$(log) python
# CUDA?=cuda:0
CUDA_VISIBLE_DEVICES=
TF_CPP_MIN_LOG_LEVEL=3
corpus_dir?=./test
output_dir?=./.data
cache_dir=./../../.cache
runner?=../../runner/target/release/runner
workers?=4
minimum_memory?=10GB
RUST_LOG?=trace
############################################################
$(output_dir) $(cache_dir):
	mkdir -p $@
	
run: | $(output_dir)
	$(python_cmd) local/run.py --input $(corpus_dir) --output $(output_dir) --workers $(workers) --memory_limit $(memory_limit) \
	    --limit_mb $(limit_mb)
.PHONY: run	

run/runner: | $(output_dir)  $(cache_dir)
	$(runner) --input $(output_dir) --output $(output_dir) --workers $(workers) \
		--cmd "python local/detect_smn.py --input {input} --output {output}" \
		--output-file audio.ina_segments --extensions wav --minimum-memory $(minimum_memory) \
		--same-dir --cache-file $(cache_dir)/.sm.cache
.PHONY: run/runner	

.EXPORT_ALL_VARIABLES:
