# twitter_search 

Python script for searching [Twitter Search API](https://developer.twitter.com/en/docs/tweets/search/api-reference/get-search-tweets.html)

## Instructions 
### Prerequisite
1. Install docker-ce
   1. [Windows](https://docs.docker.com/docker-for-windows/install/#where-to-go-next)
   2. [Ubuntu](https://docs.docker.com/engine/installation/linux/docker-ce/ubuntu/)
2. Create [Twitter app keys](https://apps.twitter.com/).

3. Create a data folder on local machine.
4. Create a `keywords.txt` file within the data folder. This file should hold keywords you want to search on Twitter, each keyword on a seperate line.
5. Create a `twitter_keys.py` file withub the data folder. The `twitter_keys.py` should have these four lines with app keys as strings.
```python
consumer_key        = '***'
consumer_secret     = '***'
access_token        = '***'
access_token_secret = '***'
```
### Running the docker image

`docker run -it -v /local/data/folder:/data rsimanaitis/twitter_search`

