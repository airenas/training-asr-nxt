-include Makefile.docker.options
#####################################################################################
service=airenas/icefall
version?=vu_hpc-0.1
docker_base_version?=torch2.4.1-cuda12.4
########### DOCKER ##################################################################
tag=$(service):$(version)

dbuild:
	docker buildx build -t $(tag) --build-arg BASE_DOCKER_VERSION=$(docker_base_version) -f Dockerfile ./ --load
.PHONY: dbuild

icefall_$(version).sif:
	apptainer build icefall_$(version).sif docker-daemon://$(tag)
build/apptainer: icefall_$(version).sif
.PHONY: build/apptainer

clean:
	docker rmi $(tag)
	rm -f icefall_$(version).sif
	apptainer clean cache
.PHONY: clean
#####################################################################################

