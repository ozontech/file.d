FROM ubuntu:19.04

WORKDIR /filed

COPY ./file-d .

CMD ./file-d
