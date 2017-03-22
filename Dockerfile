FROM continuumio/miniconda3
ADD assets/ /opt/resource/
RUN conda update conda -y
RUN conda install conda-build -y
