
FROM tensorflow/tensorflow:latest-gpu-py3
USER root
SHELL [ "/bin/bash", "-c" ]

RUN pip3 install sklearn keras sqlalchemy numpy 
COPY job_tuning.py /


