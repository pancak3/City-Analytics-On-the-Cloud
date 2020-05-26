# Copyright
   Copyright [2020] [Qifan Deng, Zijie Pan, Mandeep Singh, Steven, Tang]

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
# lastest updates:
   ## Implemented functionalities on 2020.05.26:
   - Summary and general statistics of all harvested tweets data
   - Keyword search by area name and top mentioned hashtags calculation
   - Frequency of sports and exercise related tweets
   - Analysis of correlation between sentiment of tweets and education & occupation level(AURIN IEO 2016), together with correlation between sentiment of tweets and  regional wealth.


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
