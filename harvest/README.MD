[//]: <> (@author Team 42, Chengdu, China, Qifan Deng, 1077479)

# Harvest

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
## Usage
Run help,
```python
python harvest.py -h
usage: harvest.py [-h] [-r ROLE] [-i [IP]] [-l [LOG]]

optional arguments:
  -h, --help            show this help message and exit
  -r ROLE, --role ROLE  specify the role in [reg,]
  -i [IP], --ip [IP]    IP address that Registry used to communicate with
                        workers.
  -l [LOG], --log [LOG]
                        output log level [critical, fatal, error, warning,
                        warn ,info, debug, notset]
```

Run Master,
```python
python harvest.py -r reg -i 127.0.0.1
```

Run a Worker,
```python
python harvest.py -r worker
```
## Documentations

- [cloudant](https://python-cloudant.readthedocs.io/en/latest/)
- [tweepy](http://docs.tweepy.org/en/v3.8.0/)
- [Twitter Dev](https://developer.twitter.com/en/docs)