#! /bin/bash

pprint() {
  printf "\033[32m$1\033[0m\n"
}

uninstall() {
  pprint "uninstalling file.d..."
  systemctl stop file.d
  systemctl disable file.d
  systemctl daemon-reload 
}

uninstall