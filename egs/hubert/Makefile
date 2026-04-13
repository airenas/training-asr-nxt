############################################################
-include ../../Makefile.options
log?=INFO
python_cmd=PYTHONPATH=./../../ LOG_LEVEL=$(log) python
workers?=10
n_clusters_00?=100
n_clusters_01?=500
gpu?=3
n_shards?=4
############################################################
l_output_dir?=./.data

corpus_01_dir?=$(l_output_dir)/corpus-01
corpus_02_dir?=$(l_output_dir)/corpus-02
tsv01_dir?=$(l_output_dir)/tsv-01
tsv02_dir?=$(l_output_dir)/tsv-02
base01_model_01=$(l_output_dir)/base/e01/checkpoints/checkpoint_569_25000.pt
layer01?=6
base02_model_01=$(l_output_dir)/base/e02/checkpoints/checkpoint_last.pt
layer02?=9
############################################################
$(l_output_dir) $(l_output_dir)/base/km00 $(l_output_dir)/base/labels00 \
	$(l_output_dir)/base/km01 $(l_output_dir)/base/labels01 \
	$(tsv01_dir) $(tsv02_dir):
	mkdir -p $@
############################################################
info:
	@echo "python_cmd			: $(python_cmd)"
	@echo "workers				: $(workers)"
	@echo "n_shards				: $(n_shards)"
	@echo "gpu					: $(gpu)"
	@echo "base01_model_01    	: $(base01_model_01)"
	@echo "layer01				: $(layer01)"
	@echo "base02_model_01    	: $(base02_model_01)"
	@echo "layer02				: $(layer02)"
	@echo "n_clusters_00		: $(n_clusters_00)"
	@echo "n_clusters_01		: $(n_clusters_01)"
	@echo "corpus_01_dir		: $(corpus_01_dir)"
	@echo "corpus_02_dir		: $(corpus_02_dir)"

.PHONY: info
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
corpus_dir?=$(corpus_01_dir)
load/s3/%: | $(l_output_dir)
	$(python_cmd) local/s3_dwn.py --s3-bucket audio-corpus/$* --dest-dir $(corpus_dir) \
		--s3-from $(s3_from) --s3-to $(s3_to) --workers $(workers) 

# one tar is 5 hours of audio, so 5 tars is 25 hours, which is a good chunk to process at once
$(l_output_dir)/.loaded.01: | $(l_output_dir)
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
load/01: $(l_output_dir)/.loaded.01
.PHONY: load/01
############################################################
$(tsv01_dir)/valid.tsv $(tsv01_dir)/train.tsv: $(l_output_dir)/.loaded.01 | $(tsv01_dir)
	$(python_cmd) local/make_wav_list.py --input $(corpus_01_dir) --output $(tsv01_dir)
data/01: $(tsv01_dir)/valid.tsv $(tsv01_dir)/train.tsv
.PHONY: data/01
############################################################
$(l_output_dir)/.loaded.02: | $(l_output_dir)
	make load/s3/crawl s3_from=0 s3_to=8 corpus_dir=$(corpus_02_dir)
# crawl-augmented 15h each
	make load/s3/crawl-augmented s3_from=20 s3_to=22 corpus_dir=$(corpus_02_dir)
	make load/s3/08kHz s3_from=0 s3_to=8 corpus_dir=$(corpus_02_dir)
	make load/s3/16kHz s3_from=0 s3_to=8 corpus_dir=$(corpus_02_dir)
	make load/s3/liepa3 s3_from=0 s3_to=8 corpus_dir=$(corpus_02_dir)
	make load/s3/voxlingua s3_from=0 s3_to=8 corpus_dir=$(corpus_02_dir)
# a lot of non lithuanian data, so skip
#	make load/s3/voxpopuli s3_from=0 s3_to=4 
	touch $@
load/02: $(l_output_dir)/.loaded.02	
.PHONY: load/02
############################################################
$(tsv02_dir)/valid.tsv $(tsv02_dir)/train.tsv: $(l_output_dir)/.loaded.02 | $(tsv02_dir)
	$(python_cmd) local/make_wav_list.py --input $(corpus_02_dir) --output $(tsv02_dir)	
data/02: $(tsv02_dir)/valid.tsv $(tsv02_dir)/train.tsv
.PHONY: data/02	
############################################################
############################################################
############################################################
## BASE model 
## iteration 1
############################################################
shards := $(shell seq 0 $$(($(n_shards)-1)))
features00_valid_done_files := $(foreach r,$(shards),$(l_output_dir)/.feat00.valid.$(r).done)
features00_train_done_files := $(foreach r,$(shards),$(l_output_dir)/.feat00.train.$(r).done)

$(l_output_dir)/.feat00.valid.%.done: $(tsv01_dir)/valid.tsv
	$(python_cmd) fairseq/examples/hubert/simple_kmeans/dump_mfcc_feature.py $(tsv01_dir) valid $(n_shards) $* $(l_output_dir)/base/feat00
	touch $@
$(l_output_dir)/.feat00.train.%.done: $(tsv01_dir)/train.tsv
	$(python_cmd) fairseq/examples/hubert/simple_kmeans/dump_mfcc_feature.py $(tsv01_dir) train $(n_shards) $* $(l_output_dir)/base/feat00
	touch $@
prepare/features/00: $(features00_valid_done_files) $(features00_train_done_files)
.PHONY: prepare/features/00
############################################################
$(l_output_dir)/base/km00/train: $(tsv01_dir)/train.tsv | $(l_output_dir)/base/km00
	$(python_cmd) fairseq/examples/hubert/simple_kmeans/learn_kmeans.py $(l_output_dir)/base/feat00 train $(n_shards) $(l_output_dir)/base/km00/train $(n_clusters_00) --percent 1
############################################################
valid00_done_files := $(foreach r,$(shards),$(l_output_dir)/.labels00.valid.$(r).done)
$(l_output_dir)/.labels00.valid.%.done: $(tsv01_dir)/valid.tsv $(l_output_dir)/base/km00/train | $(l_output_dir)/base/labels00
	$(python_cmd) fairseq/examples/hubert/simple_kmeans/dump_km_label.py \
		$(l_output_dir)/base/feat00 valid \
		$(l_output_dir)/base/km00/train \
		$(n_shards) $* \
		$(l_output_dir)/base/labels00
	touch $@
train00_done_files := $(foreach r,$(shards),$(l_output_dir)/.labels00.train.$(r).done)
$(l_output_dir)/.labels00.train.%.done:  $(tsv01_dir)/train.tsv $(l_output_dir)/base/km00/train | $(l_output_dir)/base/labels00
	$(python_cmd) fairseq/examples/hubert/simple_kmeans/dump_km_label.py \
		$(l_output_dir)/base/feat00 train \
		$(l_output_dir)/base/km00/train \
		$(n_shards) $* \
		$(l_output_dir)/base/labels00
	touch $@	
prepare/labels/00: $(valid00_done_files) $(train00_done_files)
.PHONY: prepare/labels/00
$(l_output_dir)/.merge00.valid: $(valid00_done_files) | $(l_output_dir)/base/labels00
	$(python_cmd) local/merge_km.py \
		--split valid \
		--n-shards $(n_shards) \
		--base-dir $(l_output_dir)/base/labels00 
	touch $@
$(l_output_dir)/.merge00.train: $(train00_done_files) | $(l_output_dir)/base/labels00
	$(python_cmd) local/merge_km.py \
		--split train \
		--n-shards $(n_shards) \
		--base-dir $(l_output_dir)/base/labels00
	touch $@
$(l_output_dir)/base/labels00/dict.km.txt: | $(l_output_dir)/base/labels00
	$(python_cmd) local/dummy_dict.py --n $(n_clusters_00) --output $@_
	mv $@_ $@
############################################################
$(l_output_dir)/.prepared.00: prepare/features/00 $(l_output_dir)/base/km00/train prepare/labels/00 $(l_output_dir)/.merge00.train \
		$(l_output_dir)/.merge00.valid $(l_output_dir)/base/labels00/dict.km.txt
	touch $@
prepare/base/00: $(l_output_dir)/.prepared.00
.PHONY: prepare/base/00
############################################################
## train for 250k updates
$(l_output_dir)/.trained.01: prepare/base/00
	torchrun --nproc_per_node=$(gpu) fairseq/fairseq_cli/hydra_train.py \
	--config-dir fairseq/examples/hubert/config/pretrain \
	--config-name hubert_base_librispeech \
	task.data=$(CURDIR)/$(tsv01_dir) task.label_dir=$(CURDIR)/$(l_output_dir)/base/labels00 task.labels='["km"]' model.label_rate=100 \
	hydra.run.dir=$(CURDIR)/$(l_output_dir)/base/e01
	touch $@
train/base/01: $(l_output_dir)/.trained.01
.PHONY: train/base/01
############################################################
### BASE model 
### iteration 2
############################################################
features01_valid_done_files := $(foreach r,$(shards),$(l_output_dir)/.feat01.valid.$(r).done)
features01_train_done_files := $(foreach r,$(shards),$(l_output_dir)/.feat01.train.$(r).done)
layer01?=6
gpu_for_0?=0
gpu_for_1?=1
gpu_for_2?=2
gpu_for_3?=0

$(l_output_dir)/.feat01.valid.%.done: $(tsv01_dir)/valid.tsv
	CUDA_VISIBLE_DEVICES=$(gpu_for_$*) $(python_cmd) fairseq/examples/hubert/simple_kmeans/dump_hubert_feature.py $(tsv01_dir) valid ${base01_model_01} ${layer01} $(n_shards) $* \
		$(l_output_dir)/base/feat01
	touch $@
$(l_output_dir)/.feat01.train.%.done: $(tsv01_dir)/train.tsv
	CUDA_VISIBLE_DEVICES=$(gpu_for_$*) $(python_cmd) fairseq/examples/hubert/simple_kmeans/dump_hubert_feature.py $(tsv01_dir) train ${base01_model_01} ${layer01} $(n_shards) $* \
		$(l_output_dir)/base/feat01
	touch $@
prepare/features/01: $(features01_valid_done_files) $(features01_train_done_files)
.PHONY: prepare/features/01
############################################################
$(l_output_dir)/base/km01/train: $(features01_train_done_files)
	$(python_cmd) fairseq/examples/hubert/simple_kmeans/learn_kmeans.py $(l_output_dir)/base/feat01 train $(n_shards) $(l_output_dir)/base/km01/train $(n_clusters_01) --percent 0.1
############################################################
############################################################
valid01_done_files := $(foreach r,$(shards),$(l_output_dir)/.labels01.valid.$(r).done)
train01_done_files := $(foreach r,$(shards),$(l_output_dir)/.labels01.train.$(r).done)

$(l_output_dir)/.labels01.valid.%.done: $(tsv01_dir)/valid.tsv $(l_output_dir)/base/km01/train | $(l_output_dir)/base/labels01
	$(python_cmd) fairseq/examples/hubert/simple_kmeans/dump_km_label.py \
		$(l_output_dir)/base/feat01 valid \
		$(l_output_dir)/base/km01/train \
		$(n_shards) $* \
		$(l_output_dir)/base/labels01
	touch $@
$(l_output_dir)/.labels01.train.%.done: $(tsv01_dir)/train.tsv $(l_output_dir)/base/km01/train | $(l_output_dir)/base/labels01
	$(python_cmd) fairseq/examples/hubert/simple_kmeans/dump_km_label.py \
		$(l_output_dir)/base/feat01 train \
		$(l_output_dir)/base/km01/train \
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
$(l_output_dir)/.prepared.01: prepare/features/01 $(l_output_dir)/base/km01/train prepare/labels/01 $(l_output_dir)/.merge01.train $(l_output_dir)/.merge01.valid $(l_output_dir)/base/labels01/dict.km.txt
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
	task.data=$(CURDIR)/$(tsv01_dir) task.label_dir=$(CURDIR)/$(l_output_dir)/base/labels01 task.labels='["km"]' model.label_rate=50 \
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
features02_valid_done_files := $(foreach r,$(shards),$(l_output_dir)/.feat02.valid.$(r).done)
features02_train_done_files := $(foreach r,$(shards),$(l_output_dir)/.feat02.train.$(r).done)

$(l_output_dir)/.feat02.valid.%.done: $(tsv02_dir)/valid.tsv
	$(python_cmd) fairseq/examples/hubert/simple_kmeans/dump_hubert_feature.py .data/tsv valid ${base02_model_01} ${layer02} $(n_shards) $* \
		$(l_output_dir)/large/feat02
	touch $@
$(l_output_dir)/.feat02.train.%.done: $(tsv02_dir)/train.tsv
	$(python_cmd) fairseq/examples/hubert/simple_kmeans/dump_hubert_feature.py .data/tsv train ${base02_model_01} ${layer02} $(n_shards) $* \
		$(l_output_dir)/large/feat02
	touch $@
prepare/features/02: $(features02_valid_done_files) $(features02_train_done_files)
.PHONY: prepare/features/02
############################################################
$(l_output_dir)/large/km02/train: $(features02_train_done_files)
	$(python_cmd) fairseq/examples/hubert/simple_kmeans/learn_kmeans.py $(l_output_dir)/large/feat02 train $(n_shards) $(l_output_dir)/large/km02/train $(n_clusters_01) --percent 0.1
prepare/km/02: $(l_output_dir)/large/km02/train
.PHONY: prepare/km/02
############################################################
############################################################
valid02_done_files := $(foreach r,$(shards),$(l_output_dir)/.labels02.valid.$(r).done)
$(l_output_dir)/.labels02.valid.%.done: $(tsv02_dir)/valid.tsv $(l_output_dir)/large/km02/train | $(l_output_dir)/large/labels02
	$(python_cmd) fairseq/examples/hubert/simple_kmeans/dump_km_label.py \
		$(l_output_dir)/large/feat02 valid \
		$(l_output_dir)/large/km02/train \
		$(n_shards) $* \
		$(l_output_dir)/large/labels02
	touch $@
train02_done_files := $(foreach r,$(shards),$(l_output_dir)/.labels02.train.$(r).done)
$(l_output_dir)/.labels02.train.%.done: $(tsv02_dir)/train.tsv $(l_output_dir)/large/km02/train | $(l_output_dir)/large/labels02
	$(python_cmd) fairseq/examples/hubert/simple_kmeans/dump_km_label.py \
		$(l_output_dir)/large/feat02 train \
		$(l_output_dir)/large/km02/train \
		$(n_shards) $* \
		$(l_output_dir)/large/labels02
	touch $@	
prepare/labels/02: $(valid02_done_files) $(train02_done_files)
.PHONY: prepare/labels/02
$(l_output_dir)/.merge02.valid: $(valid02_done_files) | $(l_output_dir)/large/labels02
	$(python_cmd) local/merge_km.py \
		--split valid \
		--n-shards $(n_shards) \
		--base-dir $(l_output_dir)/large/labels02 
	touch $@
$(l_output_dir)/.merge02.train: $(train02_done_files) | $(l_output_dir)/large/labels02
	$(python_cmd) local/merge_km.py \
		--split train \
		--n-shards $(n_shards) \
		--base-dir $(l_output_dir)/large/labels02
	touch $@
$(l_output_dir)/large/labels02/dict.km.txt: | $(l_output_dir)/large/labels02
	$(python_cmd) local/dummy_dict.py --n $(n_clusters_01) --output $@_
	mv $@_ $@
############################################################
$(l_output_dir)/.prepared.02: prepare/features/02 prepare/km/02 prepare/labels/02 $(l_output_dir)/.merge02.train $(l_output_dir)/.merge02.valid $(l_output_dir)/large/labels02/dict.km.txt
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
	task.data=$(CURDIR)/$(l_output_dir)/tsv task.label_dir=$(CURDIR)/$(l_output_dir)/large/labels02 task.labels='["km"]' model.label_rate=50 \
	hydra.run.dir=$(CURDIR)/$(l_output_dir)/large/e03
	touch $@
train/large/03: $(l_output_dir)/.trained.03
.PHONY: train/large/03
############################################################
clean:
	@echo -n "Are you sure you want to delete $(l_output_dir)/...? [y/N] " && read ans && [ $${ans:-N} = y ]
	rm -rf $(l_output_dir)/.loaded $(tsv01_dir) $(tsv02_dir)

.EXPORT_ALL_VARIABLES:
