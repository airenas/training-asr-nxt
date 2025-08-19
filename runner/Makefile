-include Makefile.options
workers?=4
PYTHONPATH=./../
LOG_LEVEL?=info
CUDA_VISIBLE_DEVICES=
###############################################################################
run:
	RUST_LOG=trace cargo run --bin runner -- --input ../test1 --output \
	../.calc-data --workers $(workers) \
	--cmd "python ../egs/sm_detection_ina/local/detect_smn.py --input {input} --output {output}"
.PHONY: run
###############################################################################
build/local: 
	cargo build --release --bin runner --bin zipper --bin db-filler --bin file-to-db
.PHONY: build/local
###############################################################################
run/zipper:
	RUST_LOG=trace cargo run --bin zipper -- --input=../.calc-data --output-file=1.zip \
	--names="audio.rttm,audio.ina_segments"
.PHONY: run/zipper
###############################################################################
run/filler:
	RUST_LOG=$(LOG_LEVEL) cargo run --bin db-filler -- --input=./../test --workers $(workers) \
	--extensions="m4a" --audio-base=./../.calc-data
.PHONY: run/filler
###############################################################################
run/file-to-db:
	RUST_LOG=$(LOG_LEVEL) cargo run --bin file-to-db -- --workers $(workers) \
	--names="audio.rttm" --input-base=./../.calc-data
.PHONY: run/file-to-db
###############################################################################
run/db-to-file:
	RUST_LOG=$(LOG_LEVEL) cargo run --bin db-to-file -- --workers $(workers) \
	--names="audio.rttm" --output-base=./../.t1
.PHONY: run/db-to-file
###############################################################################
test/unit:
	RUST_LOG=DEBUG cargo test --no-fail-fast
.PHONY: test/unit		
test/lint:
	@cargo clippy -V
	cargo clippy --all-targets --all-features -- -D warnings
.PHONY: test/lint	
###############################################################################
clean:
	rm -r -f target
.PHONY: clean

.EXPORT_ALL_VARIABLES:
