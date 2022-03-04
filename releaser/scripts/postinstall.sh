#! /bin/bash

pprint() {
  printf "\033[32m$1\033[0m\n"
}

install() {
  pprint "file.d clean install..."
  pprint "reload the service unit from disk..."
  systemctl daemon-reload 
  pprint "unmask the service..."
  systemctl unmask file.d
  pprint "enable file.d on system startup..."
  systemctl enable file.d
  systemctl restart file.d
}

install