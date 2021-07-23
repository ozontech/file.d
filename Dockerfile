FROM ubuntu:20.04

WORKDIR /file.d

COPY ./file.d .

RUN apt-get update
RUN apt-get install systemd -y

CMD ./file.d
