ARG BASE_DOCKER_VERSION=torch2.4.1-cuda12.4
#########################################################################
FROM k2fsa/icefall:${BASE_DOCKER_VERSION}
# switch to fork and update code
RUN git remote set-url origin https://github.com/airenas/icefall.git \
	&& git fetch origin \
	&& git checkout -B airenas-main origin/master
# install local
RUN pip install -e .

