############################################################
-include Makefile.options
############################################################
log?=INFO
python_cmd=PYTHONPATH=./ LOG_LEVEL=$(log) python
CUDA?=cuda:0
corpus_dir?=./test
output_dir?=./.data
workers?=4
############################################################
install/req:
	# conda create --name tasr python=3.12
	pip install -r requirements.txt
	## don't work in requirements.txt ?!
	## pip install --cache-dir ..... torchaudio torch speechbrain


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
	$(python_cmd) preparation/run.py --input $(corpus_dir) --output $(output_dir) --workers $(workers)
.PHONY: run	
