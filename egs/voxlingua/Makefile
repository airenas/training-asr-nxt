############################################################
-include ../../Makefile.options
############################################################
voxlingua_dir?=./test
output_dir?=./.data
git_dir?=./voxpopuli
fs-runner?=../../runner/target/release/fs-runner
workers?=4
RUST_LOG?=trace
############################################################
$(output_dir) $(voxlingua_dir):
	mkdir -p $@

get: | $(voxlingua_dir)
	wget https://cs.taltech.ee/staff/tanel.alumae/data/voxlingua107/lt.zip -O $(voxlingua_dir)/lt.zip
	unzip $(voxlingua_dir)/lt.zip -d $(voxlingua_dir)
.PHONY: get

.EXPORT_ALL_VARIABLES:
