FROM jodafons/root-cern:base


LABEL maintainer "Joao Victor da Fonseca Pinto <jodafons@lps.ufrj.br>"

USER root
ENV LC_ALL C.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV TERM screen

COPY setup_envs.sh /setup_envs.sh
COPY entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]

