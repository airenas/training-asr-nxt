############################################################
-include ../../Makefile.options
############################################################
voxpopuli_dir?=./test
output_dir?=./.data
git_dir?=./voxpopuli
fs-runner?=../../runner/target/release/fs-runner
workers?=4
RUST_LOG?=trace
############################################################
$(output_dir) $(voxpopuli_dir)/v1 $(voxpopuli_dir)/v2:
	mkdir -p $@

$(git_dir):
	git clone https://github.com/facebookresearch/voxpopuli.git $(git_dir)

# conda create --name vp python=3.10
requirements: $(git_dir) 
	pip install --cache-dir $(cache_dir) torchaudio==0.13.0 "numpy<2"
	pip install --cache-dir $(cache_dir) -r $(git_dir)/requirements.txt
.PHONY: requirements	

get/v1: | $(git_dir) $(voxpopuli_dir)/v1
	cd $(git_dir) && python -m voxpopuli.download_audios --root $(voxpopuli_dir)/v1 --subset lt
.PHONY: get/v1	

get/v2: | $(git_dir) $(voxpopuli_dir)/v2
	cd $(git_dir) && python -m voxpopuli.download_audios --root $(voxpopuli_dir)/v2 --subset lt_v2
.PHONY: get/v2

.EXPORT_ALL_VARIABLES:
