# analysis

## Dependencies
- python3
- pip3

## Installation
Install python libraries,
```
pip3 install -r requirements.txt
```
Configure CouchDB credentials,
```
cat couchdb.json 
{
  "protocol": "http",
  "host": "127.0.0.1",
  "port": 5984,
  "username": "admin",
  "password": "password"
}
``` 

## Documentation
- [cloudant](https://python-cloudant.readthedocs.io/en/latest/)
