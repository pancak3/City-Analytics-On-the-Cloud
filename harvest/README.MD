# harvest

## Dependencies
- python3
- pip3

## Installation
Install python libraries,
```
pip3 install -r requirements.txt
```
Configure Twitter API and CouchDB credentials,
```
cat couchdb.json 
{
  "protocol": "http",
  "host": "127.0.0.1",
  "port": 5984,
  "username": "admin",
  "password": "password"
}

cat twitter.json 
[
  {
    "api_key": "api_key_0",
    "api_secret_key": "api_secret_key_0",
    "access_token": "access_token_0",
    "access_token_secret": "access_token_secret_0"
  },
  {
    "api_key": "api_key_1",
    "api_secret_key": "api_secret_key_1",
    "access_token": "access_token_1",
    "access_token_secret": "access_token_secret_1"
  },
  ... ...
]

``` 

## Documentations

- [cloudant](https://python-cloudant.readthedocs.io/en/latest/cloudant.html)
- [python-twitter](https://python-twitter.readthedocs.io/en/latest/twitter.html)
- [Twitter Dev](https://developer.twitter.com/en/docs)