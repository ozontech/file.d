FROM golang:1.12

WORKDIR /filed

COPY ./file-d .

CMD ./file-d
