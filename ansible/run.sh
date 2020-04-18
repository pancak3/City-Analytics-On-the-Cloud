#!/bin/bash

# Path to private key
KEY_FILE=~/.ssh/steven
# Key name for VM
KEY_NAME=Steven

chmod +x unimelb-comp90024-2020-grp-42-openrc.sh
. ./unimelb-comp90024-2020-grp-42-openrc.sh

# Run from start
# ANSIBLE_HOST_KEY_CHECKING=False ansible-playbook --ask-become-pass \
#     -u ubuntu --key-file=$KEY_FILE \
#     -e "ansible_python_interpreter=/usr/bin/python3 key_name=$KEY_NAME" \
#     --tags all \
#     main.yaml

# Only run configure playbook
ANSIBLE_HOST_KEY_CHECKING=False ansible-playbook --ask-become-pass \
    -u ubuntu --key-file=$KEY_FILE \
    -e "ansible_python_interpreter=/usr/bin/python3 key_name=$KEY_NAME" \
    --tags instance_info --tags server-setup --tags configure \
    main.yaml
