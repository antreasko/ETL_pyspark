FROM ubuntu:20.04

RUN apt update
RUN apt -y upgrade
RUN apt install -y python3.8 python3-pip
RUN apt install -y openjdk-14-jdk-headless

ENV PORT=8080

COPY . /app/
WORKDIR /app/

RUN pip3 install -r requirements.txt


CMD python3 final_task.py
