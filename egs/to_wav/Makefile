############################################################
-include ../../Makefile.options
############################################################
corpus_dir?=./test
output_dir?=./.data
fs-runner?=../../runner/target/release/fs-runner
workers?=4
RUST_LOG?=trace
############################################################
$(output_dir):
	mkdir -p $@
	
run/runner: | $(output_dir)
	$(fs-runner) --input $(corpus_dir) --output $(output_dir) --workers $(workers) \
		--cmd "ffmpeg -y -i {input} -ar 16000 -ac 1 -f wav {output}" \
		--output-file audio.16.wav --extensions m4a,wav,mp3,ogg
.PHONY: run/runner	

run/runner/voxlingua: | $(output_dir)
	$(MAKE) corpus_dir=$(voxlingua_dir) output_dir=$(output_dir)/voxlingua run/runner
.PHONY: run/runner/voxlingua	

run/runner/voxpopuli: | $(output_dir)
	$(MAKE) corpus_dir=$(voxpopuli_dir) output_dir=$(output_dir)/voxpopuli run/runner
.PHONY: run/runner/voxpopuli


.EXPORT_ALL_VARIABLES:
