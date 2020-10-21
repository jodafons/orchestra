FROM tensorflow/tensorflow:2.3.1-gpu


LABEL maintainer "Joao Victor da Fonseca Pinto <jodafons@lps.ufrj.br>"

ENV LC_ALL C.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV TERM screen
USER root
SHELL [ "/bin/bash", "-c" ]

RUN apt-get update
RUN apt-get install -y git vim 


# Install Gaugi
RUN pip install future \
                numpy \
                onnxruntime \
                Flask_Security \
                passlib \
                PTable \
                Flask_Mail \
                numpy \
                requests \
                Werkzeug \
                SQLAlchemy \
                Jinja2 \
                Flask \
                Flask_Login \
                Flask_SQLAlchemy \
                BeneDict \
                flask_admin \
                flask_bootstrap \
                flask_cors \
                flask_restful \
                prettytable \
                sqlalchemy_utils \
                psycopg2-binary \
                ansi2html \
                sklearn \
                pandas




COPY test_gpu.py /test_gpu.py
COPY setup_envs.sh /
RUN chmod 777 /setup_envs.sh
