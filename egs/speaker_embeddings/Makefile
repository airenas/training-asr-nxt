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
############################################################
install/req:
	# conda create --name tasr python=3.13
	pip install -r requirements.txt
############################################################
$(output_dir):
	mkdir -p $@
	
run: | $(output_dir)
	$(python_cmd) local/embeddings.py  --input $(corpus_dir) --output $(output_dir)
.PHONY: run	

run/runner: | $(output_dir)
	$(runner) --input $(output_dir) --output $(output_dir) --workers $(workers) \
		--cmd "python local/embeddings.py --input {input} --output {output}" \
		--output-file speaker.embeddings.jsonb --extensions audio.rttm --minimum-memory $(minimum_memory) \
		--same-dir
.PHONY: run/runner	

.EXPORT_ALL_VARIABLES:
