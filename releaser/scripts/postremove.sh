#! /bin/bash

pprint() {
  printf "\033[32m$1\033[0m\n"
}

postuninstall() {
  pprint "running post-install script for file.d..."
  systemctl reset-failed
}

postuninstall