FROM ubuntu:19.04

WORKDIR /file-d

COPY ./file-d .

CMD ./file-d
