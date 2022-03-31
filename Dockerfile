# syntax=docker/dockerfile:1

FROM python:3.10.4-slim-bullseye

# install git
RUN apt-get update && apt-get install -y \
  git \
  build-essential \
  linux-headers-generic

# Clone repo
RUN git clone https://github.com/SAMTOMINDUSTRYS/stex2s-python.git
WORKDIR stex2s-python
RUN python -m pip install -r requirements.txt
WORKDIR stexs-py
RUN python -m pip install .

ENV STEX_EXCHANGE_HOST='0.0.0.0'
ENV STEX_EXCHANGE_PORT=5412
EXPOSE 5412
CMD [ "python3", "stexs/entrypoints/main_exchange.py"]

