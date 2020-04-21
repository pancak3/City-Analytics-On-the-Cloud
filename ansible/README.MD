# Ansible Configuration

## Dependencies
- python3
- pip3

## Installation

Install `ansible` and `openstack`
```
pip install -r requirements.txt
```

Create a file named `ssh_key.config`,
```
cat ssh_key.config
path/to/your/secret/key
NameOfKeyPairOnOpenstack
```

Then, reset your OpenStack password [here](https://dashboard.rc.nectar.org.au/settings/reset-password/),
download your `unimelb-comp90024-2020-grp-42-openrc.sh` [here](https://dashboard.rc.nectar.org.au/project/api_access/openrc/), and put it under this directory.
