#!/bin/bash

chmod +x unimelb-comp90024-2020-grp-42-openrc.sh
. ./unimelb-comp90024-2020-grp-42-openrc.sh

ansible-playbook --ask-become-pass openstack-mrc.yaml
