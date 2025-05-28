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
workers?=4
memory_limit?=0
limit_mb?=50
runner?=../../runner/target/release/runner
minimum_memory?=5GB
PYTHONPATH=./../../
LOG_LEVEL=$(log)
RUST_LOG?=trace
cache_dir=./../../.cache


cpu?=60
OMP_NUM_THREADS=$(cpu)
OPENBLAS_NUM_THREADS=$(cpu)
MKL_NUM_THREADS=$(cpu)
NUMEXPR_NUM_THREADS=$(cpu)
############################################################
$(output_dir) $(cache_dir):
	mkdir -p $@
	
run: | $(output_dir)
# 	$(python_cmd) local/run.py --input $(corpus_dir) --output $(output_dir) --workers $(workers) --memory_limit $(memory_limit) \
# 	    --limit_mb $(limit_mb)
	$(python_cmd) local/pyannote.py  --input $(in) --output $(out)
.PHONY: run	

run/runner: | $(output_dir) $(cache_dir)
	$(runner) --input $(output_dir) --output $(output_dir) --workers $(workers) \
		--cmd "python local/pyannote.py --input {input} --output {output}" \
		--output-file audio.rttm --extensions wav,audio.ina_segments --minimum-memory $(minimum_memory) \
		--same-dir --cache-file $(cache_dir)/.diarization.cache
.PHONY: run/runner	

run/runner/grid: | $(output_dir) $(cache_dir)
	$(runner) --input $(output_dir) --output $(output_dir) --workers $(workers) \
		--cmd "python local/to_textgrid.py --input {input} --output {output}" \
		--output-file audio.textGrid --extensions audio.rttm --minimum-memory $(minimum_memory) \
		--same-dir --cache-file $(cache_dir)/.grid.cache
.PHONY: run/runner/grid

.EXPORT_ALL_VARIABLES:
