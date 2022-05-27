FROM gitlab-registry.ozon.ru/docker/ubuntu-minbase:bionic

RUN apt-get update
RUN apt-get install systemd -y

WORKDIR /file.d

COPY ./file.d .

CMD [ "./file.d" ]
