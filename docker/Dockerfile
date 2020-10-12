FROM jodafons/root

RUN useradd --user-group --system --create-home --no-log-init jodafons
RUN usermod -aG sudo jodafons

LABEL maintainer "Joao Victor da Fonseca Pinto <jodafons@lps.ufrj.br>"

USER root
ENV LC_ALL C.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV TERM screen

RUN pip install --upgrade gaugi saphyra
RUN mkdir /codes
RUN cd /codes && git clone https://github.com/jodafons/orchestra.git
RUN cd /codes && git clone https://github.com/jodafons/prometheus.git && cd prometheus && mkdir build && cd build && cmake .. && make
RUN chmod -R 777 /codes

COPY run_tuning.py /run_tuning.py
COPY entrypoint.sh /entrypoint.sh

RUN chmod 777 /setup_envs.sh
ENTRYPOINT ["/bin/bash","entrypoint.sh"]

