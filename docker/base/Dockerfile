FROM tensorflow/tensorflow:2.3.1-gpu


LABEL maintainer "Joao Victor da Fonseca Pinto <jodafons@lps.ufrj.br>"

ENV LC_ALL C.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV TERM screen
USER root
SHELL [ "/bin/bash", "-c" ]


RUN apt-get update --fix-missing
RUN apt-get install -y python3.7 python3-pip
RUN apt-get install -y git \
                       wget \
                       dpkg-dev \
                       g++ \
                       gcc \
                       binutils \
                       libx11-dev \
                       libxpm-dev \
                       libxft-dev \
                       libxext-dev \
                       gfortran \
                       libssl-dev \
                       libpcre3-dev \
                       libglew1.5-dev \
                       libftgl-dev \
                       libldap2-dev \
                       python-dev \
                       libxml2-dev \
                       libkrb5-dev \
                       libgsl0-dev \
                       libqt4-dev \
                       libfftw3-dev \
                       libcfitsio-dev \
                       graphviz-dev \
                       libavahi-compat-libdnssd-dev \
                       libboost-all-dev \
                       vim \
                       librange-v3-dev \
                       libboost-python-dev


RUN git clone https://github.com/amix/vimrc.git ${HOME}/.vim_runtime
RUN cd ${HOME}/.vim_runtime/ && source install_awesome_vimrc.sh 
RUN wget https://cmake.org/files/v3.17/cmake-3.17.2-Linux-x86_64.sh -P /usr/
RUN chmod 755 /usr/cmake-3.17.2-Linux-x86_64.sh
RUN cd /usr && ./cmake-3.17.2-Linux-x86_64.sh --skip-license
RUN ln -sf /usr/bin/python3.6 /usr/bin/python



# Install ROOT
RUN CPU_N=$(grep -c ^processor /proc/cpuinfo) && cd /opt && git clone https://github.com/root-project/root.git && \
cd root && git checkout v6-16-00 && mkdir buildthis && cd buildthis && cmake  --Dpython_version=3 .. && make -j$CPU_N


# remove weight from root
RUN cd /opt && mkdir buildthis && cd buildthis && mv ../root/buildthis/lib . && mv ../root/buildthis/include . && mv ../root/buildthis/bin . && mv ../root/buildthis/etc .
RUN cd /opt && rm -rf root && mkdir root && cd root && mv ../buildthis .


RUN ln -sf /usr/bin/pip3 /usr/bin/pip


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
                ansi2html



# Install Latex
RUN ln -snf /usr/share/zoneinfo/Etc/UTC /etc/localtime \
    && echo "Etc/UTC" > /etc/timezone \
    && apt-get update \
    && apt-get upgrade -y \
    && apt-get install texlive-latex-base texlive-latex-extra texlive-fonts-recommended xzdec -y \
    && rm -rf /var/lib/apt/lists/*

COPY test_gpu.py /test_gpu.py
COPY setup_envs.sh /
RUN chmod 777 /setup_envs.sh
