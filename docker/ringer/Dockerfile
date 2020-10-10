FROM jodafons/root:latest

LABEL maintainer "João Victor da Fonseca Pinto <jodafons@lps.ufrj.br>"

ENV LC_ALL C.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV TERM screen

USER root
SHELL [ "/bin/bash", "-c" ]
RUN pip install saphyra
RUN cd /opt && git clone https://github.com/jodafons/orchestra.git && cd orchestra && pip install .
COPY run_tuning.py /run_tuning.py

ENTRYPOINT ["maestro.py"]
