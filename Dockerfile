FROM torchelastic/torchelastic:0.1.0rc1 
RUN pip install classy-vision

WORKDIR /workspace
COPY . /workspace/elastic
RUN chmod -R u+x /workspace/elastic/examples/bin
ENV PATH=/workspace/elastic/examples/bin:${PATH}
