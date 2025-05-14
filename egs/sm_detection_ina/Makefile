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
runner?=../../runner/target/release/runner
workers?=4
minimum_memory?=10
limit_mb?=50
RUST_LOG?=trace
############################################################
install/req:
	# conda create --name tasr python=3.13
	pip install -r requirements.txt

test/unit:
	PYTHONPATH=./ pytest -v --log-level=INFO

test/lint:
	# stop the build if there are Python syntax errors or undefined names
	flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics
	#exit-zero treats all errors as warnings. The GitHub editor is 127 chars wide
	flake8 . --count --max-complexity=10 --max-line-length=127 --statistics
############################################################
$(output_dir):
	mkdir -p $@
	
run: | $(output_dir)
	$(python_cmd) local/run.py --input $(corpus_dir) --output $(output_dir) --workers $(workers) --memory_limit $(memory_limit) \
	    --limit_mb $(limit_mb)
.PHONY: run	

run/runner: | $(output_dir)
	$(runner) --input $(output_dir) --output $(output_dir) --workers $(workers) \
		--cmd "python local/detect_smn.py --input {input} --output {output}" \
		--output-file audio.ina_segments --extensions wav --minimum-memory $(minimum_memory) \
		--same-dir
.PHONY: run/runner	

.EXPORT_ALL_VARIABLES:
