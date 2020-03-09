ARG BASE_IMAGE=pytorch/elastic:latest
FROM $BASE_IMAGE

ARG WITH_CLASSY_VISION=1

RUN apt-get update && apt-get install -y python-pip
# install utilities and dependencies
RUN pip install awscli --upgrade
RUN pip install classy-vision

WORKDIR /workspace
COPY . ./examples
RUN chmod -R u+x ./examples/bin
ENV PATH=/workspace/examples/bin:${PATH}

USER nobody
ENTRYPOINT ["/workspace/examples/bin/fetch_and_run"]
