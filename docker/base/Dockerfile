FROM jodafons/root


LABEL maintainer "Joao Victor da Fonseca Pinto <jodafons@lps.ufrj.br>"

USER root
ENV LC_ALL C.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV TERM screen

RUN pip install Flask_Security \
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
                ansi2html

RUN pip install --upgrade gaugi saphyra
COPY setup_envs.sh /setup_envs.sh
RUN chmod 777 /setup_envs.sh





