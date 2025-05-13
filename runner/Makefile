-include Makefile.options
workers?=4
PYTHONPATH=./../
LOG_LEVEL=info
CUDA_VISIBLE_DEVICES=
###############################################################################
run:
	RUST_LOG=trace cargo run --bin runner -- --input ../test1 --output \
	../.calc-data --workers $(workers) \
	--cmd "python ../egs/sm_detection_ina/local/detect_smn.py --input {input} --output {output}"
.PHONY: run
###############################################################################
build/local: 
	cargo build --release --bin runner
.PHONY: build/local
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
