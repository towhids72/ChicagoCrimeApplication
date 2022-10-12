FROM python:3.10

ENV PYTHONUNBUFFERED 1
ENV C_FORCE_ROOT true
RUN mkdir /src
COPY ./requirements.txt /src

RUN pip install -r /src/requirements.txt
WORKDIR /src
COPY ./ /src
