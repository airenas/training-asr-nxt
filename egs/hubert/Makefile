############################################################
-include ../../Makefile.options
log?=INFO
python_cmd=PYTHONPATH=./../../ LOG_LEVEL=$(log) python
workers?=10
n_shard?=1
n_clusters_00?=100
n_clusters_01?=500
############################################################
l_output_dir?=./.data
############################################################
$(l_output_dir) $(l_output_dir)/base/km $(l_output_dir)/base/labels $(l_output_dir)/tsv $(l_output_dir)/base/labels01:
	mkdir -p $@
############################################################
############################################################
############################################################
### Install
############################################################
install/torch:
	# conda create --name fs python=3.10
	pip install "pip<24.1"
	pip install soundfile npy-append-array
	pip install torch torchaudio==2.4.0
	sudo apt install libsox-dev
.PHONY: install/torch
fairseq:
	git clone https://github.com/pytorch/fairseq
install/fairseq: | fairseq
	cd fairseq && pip install --editable ./
.PHONY: install/fairseq
# can not install apex
# apex:
# 	git clone https://github.com/NVIDIA/apex	
# install/apex: | apex
# 	cd apex && pip install -v --no-cache-dir --global-option="--cpp_ext" --global-option="--cuda_ext" \
# 	--global-option="--deprecated_fused_adam" --global-option="--xentropy" \
# 	--global-option="--fast_multihead_attn" --no-build-isolation ./
# .PHONY: install/apex
install/other:
	# fix
	pip install numpy==1.26.4
	pip install tensorboard tensorboardX boto3
install: install/torch install/fairseq install/other
.PHONY: install	
############################################################
############################################################
############################################################
### DATA
############################################################
s3_from?=0
s3_to?=5
load/s3/%: | $(l_output_dir)
	$(python_cmd) local/s3_dwn.py --s3-bucket audio-corpus/$* --dest-dir $(l_output_dir)/corpus \
		--s3-from $(s3_from) --s3-to $(s3_to) --workers $(workers) 

# one tar is 5 hours of audio, so 5 tars is 25 hours, which is a good chunk to process at once
$(l_output_dir)/.loaded.100: | $(l_output_dir)
	make load/s3/crawl s3_from=0 s3_to=4 
# crawl-augmented 15h each
	make load/s3/crawl-augmented s3_from=20 s3_to=21 
	make load/s3/08kHz s3_from=0 s3_to=4 
	make load/s3/16kHz s3_from=0 s3_to=4 
	make load/s3/liepa3 s3_from=0 s3_to=4 
	make load/s3/voxlingua s3_from=0 s3_to=4 
# a lot of non lithuanian data, so skip
#	make load/s3/voxpopuli s3_from=0 s3_to=4 
	
	touch $@
load/100: $(l_output_dir)/.loaded.100
.PHONY: load/100
############################################################
$(l_output_dir)/tsv/valid.tsv $(l_output_dir)/tsv/train.tsv: $(l_output_dir)/.loaded.100 | $(l_output_dir)/tsv
	$(python_cmd) local/make_wav_list.py --input $(l_output_dir)/corpus --output $(l_output_dir)/tsv
############################################################
############################################################
############################################################
## BASE model 
## iteration 1
############################################################
$(l_output_dir)/.feat00.valid.done: $(l_output_dir)/tsv/valid.tsv
	$(python_cmd) fairseq/examples/hubert/simple_kmeans/dump_mfcc_feature.py .data/tsv valid 1 0 $(l_output_dir)/base/feat00
	touch $@
$(l_output_dir)/.feat00.train.done: $(l_output_dir)/tsv/train.tsv
	$(python_cmd) fairseq/examples/hubert/simple_kmeans/dump_mfcc_feature.py .data/tsv train 1 0 $(l_output_dir)/base/feat00
	touch $@
prepare/features/mfcc: $(l_output_dir)/.feat00.valid.done $(l_output_dir)/.feat00.train.done
.PHONY: prepare/features/mfcc
############################################################
$(l_output_dir)/base/km/train$(n_clusters): $(l_output_dir)/tsv/train.tsv | $(l_output_dir)/base/km
	$(python_cmd) fairseq/examples/hubert/simple_kmeans/learn_kmeans.py $(l_output_dir)/base/feat00 train 1 $(l_output_dir)/base/km/train$(n_clusters) $(n_clusters) --percent 1
prepare/km: $(l_output_dir)/base/km/train$(n_clusters)
.PHONY: prepare/km
############################################################
$(l_output_dir)/.labels.valid.done: $(l_output_dir)/tsv/valid.tsv $(l_output_dir)/base/km/train$(n_clusters) | $(l_output_dir)/base/labels
	$(python_cmd) fairseq/examples/hubert/simple_kmeans/dump_km_label.py $(l_output_dir)/base/feat00 valid $(l_output_dir)/base/km/train$(n_clusters) 1 0 $(l_output_dir)/base/labels
	touch $@
$(l_output_dir)/.labels.train.done: $(l_output_dir)/tsv/train.tsv $(l_output_dir)/base/km/train$(n_clusters) | $(l_output_dir)/base/labels
	$(python_cmd) fairseq/examples/hubert/simple_kmeans/dump_km_label.py $(l_output_dir)/base/feat00 train $(l_output_dir)/base/km/train$(n_clusters) 1 0 $(l_output_dir)/base/labels
	touch $@
prepare/labels: $(l_output_dir)/.labels.valid.done $(l_output_dir)/.labels.train.done
.PHONY: prepare/labels
############################################################
merge/%/km: $(l_output_dir)/base/labels
	for rank in $(shell seq 0 $$(($(n_shard) - 1))); do \
		cat $(l_output_dir)/base/labels/$*_$${rank}_$(n_shard).km; \
	done > $(l_output_dir)/base/labels/$*.km
merge/all: merge/valid/km merge/train/km
.PHONY: merge/all
$(l_output_dir)/base/labels/dict.km.txt: | $(l_output_dir)/base/labels
	$(python_cmd) local/dummy_dict.py --n $(n_clusters_00) --output $@_
	mv $@_ $@
############################################################
$(l_output_dir)/.prepared.00: prepare/features/mfcc prepare/km prepare/labels merge/all $(l_output_dir)/base/labels/dict.km.txt
	touch $@
prepare/base/00: $(l_output_dir)/.prepared.00
.PHONY: prepare/base/00
############################################################
## train for 250k updates
$(l_output_dir)/.trained.01: prepare/base/00
	torchrun --nproc_per_node=3 fairseq/fairseq_cli/hydra_train.py \
	--config-dir fairseq/examples/hubert/config/pretrain \
	--config-name hubert_base_librispeech \
	task.data=$(CURDIR)/$(l_output_dir)/tsv task.label_dir=$(CURDIR)/$(l_output_dir)/base/labels task.labels='["km"]' model.label_rate=100 \
	hydra.run.dir=$(CURDIR)/$(l_output_dir)/base/e01
	touch $@
train/base/01: $(l_output_dir)/.trained.01
.PHONY: train/base/01
############################################################
### BASE model 
### iteration 2
############################################################
n_shards?=4
shards := $(shell seq 0 $$(($(n_shards)-1)))
base01_model_01=$(l_output_dir)/base/e01/checkpoints/checkpoint_1137_50000.pt
features01_valid_done_files := $(foreach r,$(shards),$(l_output_dir)/.feat01.valid.$(r).done)
features01_train_done_files := $(foreach r,$(shards),$(l_output_dir)/.feat01.train.$(r).done)
layer?=6

$(l_output_dir)/.feat01.valid.%.done: $(l_output_dir)/tsv/valid.tsv
	$(python_cmd) fairseq/examples/hubert/simple_kmeans/dump_hubert_feature.py .data/tsv valid ${base01_model_01} ${layer} $(n_shards) $* \
		$(l_output_dir)/base/feat01
	touch $@
$(l_output_dir)/.feat01.train.%.done: $(l_output_dir)/tsv/train.tsv
	$(python_cmd) fairseq/examples/hubert/simple_kmeans/dump_hubert_feature.py .data/tsv train ${base01_model_01} ${layer} $(n_shards) $* \
		$(l_output_dir)/base/feat01
	touch $@
prepare/features/01: $(features01_valid_done_files) $(features01_train_done_files)
.PHONY: prepare/features/01
############################################################
$(l_output_dir)/base/km/train$(n_clusters_01): $(features01_train_done_files)
	$(python_cmd) fairseq/examples/hubert/simple_kmeans/learn_kmeans.py $(l_output_dir)/base/feat01 train $(n_shards) $(l_output_dir)/base/km/train$(n_clusters_01) $(n_clusters_01) --percent 0.1
prepare/km/01: $(l_output_dir)/base/km/train$(n_clusters_01)
.PHONY: prepare/km/01
############################################################
############################################################
valid01_done_files := $(foreach r,$(shards),$(l_output_dir)/.labels01.valid.$(r).done)
$(l_output_dir)/.labels01.valid.%.done: $(l_output_dir)/tsv/valid.tsv $(l_output_dir)/base/km/train$(n_clusters_01) | $(l_output_dir)/base/labels01
	$(python_cmd) fairseq/examples/hubert/simple_kmeans/dump_km_label.py \
		$(l_output_dir)/base/feat01 valid \
		$(l_output_dir)/base/km/train$(n_clusters_01) \
		$(n_shards) $* \
		$(l_output_dir)/base/labels01
	touch $@
train01_done_files := $(foreach r,$(shards),$(l_output_dir)/.labels01.train.$(r).done)
$(l_output_dir)/.labels01.train.%.done: $(l_output_dir)/tsv/train.tsv $(l_output_dir)/base/km/train$(n_clusters_01) | $(l_output_dir)/base/labels01
	$(python_cmd) fairseq/examples/hubert/simple_kmeans/dump_km_label.py \
		$(l_output_dir)/base/feat01 train \
		$(l_output_dir)/base/km/train$(n_clusters_01) \
		$(n_shards) $* \
		$(l_output_dir)/base/labels01
	touch $@	
prepare/labels/01: $(valid01_done_files) $(train01_done_files)
.PHONY: prepare/labels/01
$(l_output_dir)/.merge01.valid: $(valid01_done_files) | $(l_output_dir)/base/labels01
	$(python_cmd) local/merge_km.py \
		--split valid \
		--n-shards $(n_shards) \
		--base-dir $(l_output_dir)/base/labels01 
	touch $@
$(l_output_dir)/.merge01.train: $(train01_done_files) | $(l_output_dir)/base/labels01
	$(python_cmd) local/merge_km.py \
		--split train \
		--n-shards $(n_shards) \
		--base-dir $(l_output_dir)/base/labels01
	touch $@
$(l_output_dir)/base/labels01/dict.km.txt: | $(l_output_dir)/base/labels01
	$(python_cmd) local/dummy_dict.py --n $(n_clusters_01) --output $@_
	mv $@_ $@
############################################################
$(l_output_dir)/.prepared.01: prepare/features/01 prepare/km/01 prepare/labels/01 $(l_output_dir)/.merge01.train $(l_output_dir)/.merge01.valid $(l_output_dir)/base/labels01/dict.km.txt
	touch $@
prepare/base/01: $(l_output_dir)/.prepared.01 
.PHONY: prepare/base/01
############################################################
############################################################
## train BASE for 400k updates
## Second iteration
############################################################
$(l_output_dir)/.trained.02: prepare/base/01
	torchrun --nproc_per_node=3 fairseq/fairseq_cli/hydra_train.py \
	--config-dir fairseq/examples/hubert/config/pretrain \
	--config-name hubert_base_librispeech \
	task.data=$(CURDIR)/$(l_output_dir)/tsv task.label_dir=$(CURDIR)/$(l_output_dir)/base/labels01 task.labels='["km"]' model.label_rate=50 \
	hydra.run.dir=$(CURDIR)/$(l_output_dir)/base/e02
	touch $@
train/base/02: $(l_output_dir)/.trained.02
.PHONY: train/base/02
############################################################
############################################################
############################################################
### LARGE model 
### iteration 1 (3)
############################################################
base02_model_01=$(l_output_dir)/base/e02/checkpoints/checkpoint_last.pt
features02_valid_done_files := $(foreach r,$(shards),$(l_output_dir)/.feat02.valid.$(r).done)
features02_train_done_files := $(foreach r,$(shards),$(l_output_dir)/.feat02.train.$(r).done)
layer02?=9

$(l_output_dir)/.feat02.valid.%.done: $(l_output_dir)/tsv/valid.tsv
	$(python_cmd) fairseq/examples/hubert/simple_kmeans/dump_hubert_feature.py .data/tsv valid ${base02_model_01} ${layer02} $(n_shards) $* \
		$(l_output_dir)/base/feat02
	touch $@
$(l_output_dir)/.feat02.train.%.done: $(l_output_dir)/tsv/train.tsv
	$(python_cmd) fairseq/examples/hubert/simple_kmeans/dump_hubert_feature.py .data/tsv train ${base02_model_01} ${layer02} $(n_shards) $* \
		$(l_output_dir)/base/feat02
	touch $@
prepare/features/02: $(features02_valid_done_files) $(features02_train_done_files)
.PHONY: prepare/features/02
############################################################
$(l_output_dir)/base/km/train$(n_clusters_01)_02: $(features02_train_done_files)
	$(python_cmd) fairseq/examples/hubert/simple_kmeans/learn_kmeans.py $(l_output_dir)/base/feat02 train $(n_shards) $(l_output_dir)/base/km/train$(n_clusters_01)_02 $(n_clusters_01) --percent 0.1
prepare/km/02: $(l_output_dir)/base/km/train$(n_clusters_01)_02
.PHONY: prepare/km/02
############################################################
############################################################
valid02_done_files := $(foreach r,$(shards),$(l_output_dir)/.labels02.valid.$(r).done)
$(l_output_dir)/.labels02.valid.%.done: $(l_output_dir)/tsv/valid.tsv $(l_output_dir)/base/km/train$(n_clusters_01)_02 | $(l_output_dir)/base/labels02
	$(python_cmd) fairseq/examples/hubert/simple_kmeans/dump_km_label.py \
		$(l_output_dir)/base/feat02 valid \
		$(l_output_dir)/base/km/train$(n_clusters_01)_02 \
		$(n_shards) $* \
		$(l_output_dir)/base/labels02
	touch $@
train02_done_files := $(foreach r,$(shards),$(l_output_dir)/.labels02.train.$(r).done)
$(l_output_dir)/.labels02.train.%.done: $(l_output_dir)/tsv/train.tsv $(l_output_dir)/base/km/train$(n_clusters_01)_02 | $(l_output_dir)/base/labels02
	$(python_cmd) fairseq/examples/hubert/simple_kmeans/dump_km_label.py \
		$(l_output_dir)/base/feat02 train \
		$(l_output_dir)/base/km/train$(n_clusters_01)_02 \
		$(n_shards) $* \
		$(l_output_dir)/base/labels02
	touch $@	
prepare/labels/02: $(valid02_done_files) $(train02_done_files)
.PHONY: prepare/labels/02
$(l_output_dir)/.merge02.valid: $(valid02_done_files) | $(l_output_dir)/base/labels02
	$(python_cmd) local/merge_km.py \
		--split valid \
		--n-shards $(n_shards) \
		--base-dir $(l_output_dir)/base/labels02 
	touch $@
$(l_output_dir)/.merge02.train: $(train02_done_files) | $(l_output_dir)/base/labels02
	$(python_cmd) local/merge_km.py \
		--split train \
		--n-shards $(n_shards) \
		--base-dir $(l_output_dir)/base/labels02
	touch $@
$(l_output_dir)/base/labels02/dict.km.txt: | $(l_output_dir)/base/labels02
	$(python_cmd) local/dummy_dict.py --n $(n_clusters_01) --output $@_
	mv $@_ $@
############################################################
$(l_output_dir)/.prepared.02: prepare/features/02 prepare/km/02 prepare/labels/02 $(l_output_dir)/.merge02.train $(l_output_dir)/.merge02.valid $(l_output_dir)/base/labels02/dict.km.txt
	touch $@
prepare/base/02: $(l_output_dir)/.prepared.02
.PHONY: prepare/base/02
############################################################
############################################################
## train LARGE for 400k updates
## Third iteration
############################################################
$(l_output_dir)/.trained.03: prepare/base/02
	torchrun --nproc_per_node=3 fairseq/fairseq_cli/hydra_train.py \
	--config-dir fairseq/examples/hubert/config/pretrain \
	--config-name hubert_large_librispeech \
	task.data=$(CURDIR)/$(l_output_dir)/tsv task.label_dir=$(CURDIR)/$(l_output_dir)/base/labels02 task.labels='["km"]' model.label_rate=50 \
	hydra.run.dir=$(CURDIR)/$(l_output_dir)/base/e03
	touch $@
train/large/03: $(l_output_dir)/.trained.03
.PHONY: train/large/03
############################################################
clean:
	@echo -n "Are you sure you want to delete $(l_output_dir)/...? [y/N] " && read ans && [ $${ans:-N} = y ]
	rm -rf $(l_output_dir)/.loaded $(l_output_dir)/tsv/valid.tsv $(l_output_dir)/tsv/train.tsv $(l_output_dir)/base/feat $(l_output_dir)/base/km $(l_output_dir)/base/labels
info:
	@echo "log: $(log)"
	@echo "python_cmd: $(python_cmd)"
	@echo "workers: $(workers)"
	@echo "n_shard: $(n_shard)"
	@echo "n_clusters_00: $(n_clusters_00)"
	@echo "n_clusters_01: $(n_clusters_01)"
	@echo "train01_done_files: $(train01_done_files)"

.EXPORT_ALL_VARIABLES:
