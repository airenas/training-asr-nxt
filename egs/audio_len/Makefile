############################################################
-include ../../Makefile.options
############################################################
log?=INFO
python_cmd=PYTHONPATH=./../../ LOG_LEVEL=$(log) python
CUDA_VISIBLE_DEVICES=
TF_CPP_MIN_LOG_LEVEL=3
corpus_dir?=./test
output_dir?=./.data
workers?=4
############################################################
$(output_dir):
	mkdir -p $@
	
run: | $(output_dir)
	$(python_cmd) local/run.py --input $(corpus_dir) --output $(output_dir) --workers $(workers)
.PHONY: run	

.EXPORT_ALL_VARIABLES:
