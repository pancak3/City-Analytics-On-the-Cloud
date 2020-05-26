# Mapreduce
Backing up design documents, update the design documents, update areas database and save Aurin data to the database.
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
## Usage
```python
python check_couchdb_for_api.py
```
## Documentations

- [cloudant](https://python-cloudant.readthedocs.io/en/latest/)
- [Twitter Dev](https://developer.twitter.com/en/docs)