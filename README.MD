[//]: <> (@author Team 42, Chengdu, China, Qifan Deng, 1077479)

# City Analytics on the Cloud

A Cloud-based solution which runs across multiple VM instances on Melbourne Research Cloud. <br >
Tweets were harvested across the cities of Australia by the Twitter API, then stored in a clustered CouchDB database. <br > 
Data is presented by a website which is inspired by [Covid-19 App](https://covid-dashboards.web.app/).

## Modules
There are five modules for the repository,

- Ansible - For deployment. Refer to [README.MD](ansible/README.MD) 
- APP - For both frontend and backend. Refer to [README.MD](app/README.MD)
- Harvest - For harvesting tweets and sentiment segmentation. Refer to [README.MD](harvest/README.MD) 
- Mapreduce - For backing up design documents, updating the design documents, updating areas database and saving Aurin data to the database. Refer to [README.MD](mapreduce/README.MD)
- Reverse - Reverse proxies one couch node and load balances app instances. Refer to [README.MD](reverse/README.MD)

