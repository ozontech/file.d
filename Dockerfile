FROM ubuntu:20.04

RUN apt-get update
RUN apt-get install systemd -y

WORKDIR /file.d

COPY ./file.d .

CMD ./file.d
