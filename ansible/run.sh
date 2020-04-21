#!/bin/bash

# Exit when any err occurs
set -o errexit
set -o nounset
# Path to private key
KEY_FILE=$(head -n 1 ssh_key.config)
# Key name for VM
KEY_NAME=$(tail -n 1 ssh_key.config)

chmod +x unimelb-comp90024-2020-grp-42-openrc.sh
. ./unimelb-comp90024-2020-grp-42-openrc.sh

# Only run configure playbook
ANSIBLE_HOST_KEY_CHECKING=False ansible-playbook --ask-become-pass \
    -u ubuntu --key-file=$KEY_FILE \
    -e "ansible_python_interpreter=python3 key_name=$KEY_NAME" \
    --tags instance_info --tags configure \
    main.yaml
