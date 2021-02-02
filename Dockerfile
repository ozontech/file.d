FROM ubuntu:20.04

WORKDIR /file.d

COPY ./file.d .

CMD ./file.d
