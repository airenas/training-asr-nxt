############################################################
-include ../../Makefile.options
############################################################
corpus_dir?=./test
output_dir?=./.data
runner?=../../runner/target/release/runner
workers?=4
RUST_LOG?=trace
############################################################
$(output_dir):
	mkdir -p $@
	
run/runner: | $(output_dir)
	$(runner) --input $(corpus_dir) --output $(output_dir) --workers $(workers) \
		--cmd "ffmpeg -y -i {input} -ar 16000 -ac 1 -f wav {output}" \
		--output-file audio.16.wav --extensions m4a
.PHONY: run/runner	

.EXPORT_ALL_VARIABLES:
