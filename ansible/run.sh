#!/bin/bash

# Exit when any err occurs
set -o errexit
# Path to private key
KEY_FILE=$(head -n 1 ssh_key.config)
# Key name for VM
KEY_NAME=$(tail -n 1 ssh_key.config)

# Initialise cluster
init() {
    runable
    echo "[*] Initialising ... "
    ANSIBLE_HOST_KEY_CHECKING=False ansible-playbook \
        -u ubuntu --key-file="$KEY_FILE" \
        -e "ansible_python_interpreter=python3 key_name=$KEY_NAME" \
        --tags all \
        main.yaml
}

# Configure
configure() {
    runable
    echo "[*] Configuring ... "
    ANSIBLE_HOST_KEY_CHECKING=False ansible-playbook \
        -u ubuntu --key-file="$KEY_FILE" \
        -e "ansible_python_interpreter=python3 key_name=$KEY_NAME" \
        --tags instance_info --tags configure \
        main.yaml
}

arg() {
    if [[ "$1" = "-i" ]]; then
        init
    elif [[ "$1" = "-c" ]]; then
        configure
    fi
}

runable() {
    chmod +x unimelb-comp90024-2020-grp-42-openrc.sh
    . ./unimelb-comp90024-2020-grp-42-openrc.sh
}

usage() {
    echo -e "Initialise and configure cluster:\n\t $0 -i\n"
    echo -e "Configure cluster:\n\t $0 -c\n"
}

if [[ -n "$1" ]]; then
    arg "$1"
else
    usage
fi
