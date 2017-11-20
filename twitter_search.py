# -*- coding: utf-8 -*-
"""
Created on Mon Jun  5 22:31:17 2017

@author: rytis
"""

import TwitterSearch 
import pandas as pd
import sqlite3
import pause
import logging
import time
import os
import json
import bz2
import argparse
import sys
from tqdm import tqdm
# my helper funcstions
import helpers

def twitter_search(db_file, output_dir, keywords_file):

    ts = TwitterSearch.TwitterSearch(
            consumer_key = twitter_keys.consumer_key,
            consumer_secret = twitter_keys.consumer_secret,
            access_token = twitter_keys.access_token,
            access_token_secret = twitter_keys.access_token_secret
        )
    
    start = time.time()
    window_count = 1
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    
    if keywords_file:
        keywords = helpers.get_keywords_file(keywords_file)
    else:
        keywords = helpers.get_keywords_sql(db_file)


    pbar = tqdm(keywords)
    for keyword in pbar:
        logging.debug('Getting: ' + keyword)
        # keyword = keyword.replace('/','_')
        pbar.set_description("Processing {:10}".format(keyword))
        pbar.refresh()
        
        tso = TwitterSearch.TwitterSearchOrder()
        tso.set_include_entities(True)
        tso.set_result_type('recent')
        tso.set_keywords([keyword])
        
        # only look for tweets since last search..
        c.execute('SELECT max_id FROM latest_search WHERE keyword=?',
                  [keyword])
        fetched = c.fetchone()
        since_id = fetched[0] if not fetched is None else None
        if since_id: tso.set_since_id(since_id)
        
        ts.search_tweets(tso)
        
        max_id = []
        max_date = []
        min_date = [] 
        count = []
        
        try_next = True
        while try_next:     
            # parse response
            meta = ts.get_metadata()
            remaining_limit = int(meta.get('x-rate-limit-remaining',0))            
            num_tweets = ts.get_amount_of_tweets()

            tweets = ts.get_tweets().get('statuses', [])
            helpers.write_tweets(tweets, output_dir)
            
            if num_tweets != 0:
                max_id.append(max([tweet['id'] for tweet in tweets]))
                max_date.append(max([pd.to_datetime(tweet['created_at'], utc=True) for tweet in tweets]))
                min_date.append(min([pd.to_datetime(tweet['created_at'], utc=True) for tweet in tweets]))
                count.append(num_tweets)
            
            if remaining_limit == 0:
                try:
                    limit_reset = int(meta.get('x-rate-limit-reset', time.time()+15*60)) + 10 # extra sec to be on the safe side
                    # convert to correct datetime
                    limit_reset_dt = pd.to_datetime(limit_reset, unit='s', utc=True)
                    limit_reset_dt = limit_reset_dt.tz_convert('Europe/London')
                    pbar.set_description('Sleeping until {:%H:%M:%S}'.format(limit_reset_dt))
                    pbar.refresh()
                    pause.until(limit_reset)
                    pbar.set_description("Processing %s" % keyword)
                    pbar.refresh()
                    window_count += 1
                except Exception as e:
                    logging.warn('limit_reset ERROR: '+keyword)
                    logging.warn(str(e))
                    logging.warn('Sleep for 15min...')
                     # wait the maximum time until next window...
                    pbar.set_description("Sleeping for 15 min.")
                    pbar.refresh()

                    pause.minutes(15)

                    pbar.set_description("Processing {:10}".format(keywords))
                    pbar.refresh()
                    window_count += 1

            # check if there is a next page for this search
            try:
                try_next = ts.search_next_results()
            except:
                try_next = False
    
        # stats and logging for current keyword
        max_id = max(max_id) if len(max_id) !=0 else since_id
        max_date = max(max_date) if len(max_date) !=0 else None
        min_date = min(min_date) if len(min_date) !=0 else None
        count = sum(count)
        
        search_stats ={
            'keyword':keyword,
            'count':count,
            'min_date': min_date.strftime('%Y-%m-%d %H:%M:%S') if not min_date is None else None,
            'max_date': max_date.strftime('%Y-%m-%d %H:%M:%S') if not max_date is None else None,
            'max_id': max_id,
            'search_date': pd.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            }

        helpers.dict_to_sqlite(search_stats, 'latest_search', db_file)

        
    # stats and logging for iteration
    end = time.time()
    total_time = round((end-start)/60)
    iteration_stats={
        'start_time': pd.to_datetime(start, unit='s').strftime('%Y-%m-%d %H:%M:%S'),
        'duration_min': total_time,
        'keywords': len(keywords),
        'tweets_got': ts.get_statistics()[1],
        'queries_submitted': ts.get_statistics()[0],
        'windows_used': window_count,
        }
    helpers.dict_to_sqlite(iteration_stats, 'iterations', db_file)

    logging.info('Total number of windows: ' + str(window_count))
    logging.info('Total time (min): ' + str(total_time))
    logging.info('Total tweets got: ' + str(ts.get_statistics()[1]))

    # close db file
    c.close()
    conn.close()


if __name__ == "__main__":
    # 
    # Setup
    # 
    parser = argparse.ArgumentParser(description='Twitter scraper using Twitter search API.')

    parser.add_argument('--keys', default='/data/twitter_keys',
                        help = 'Specify the .py file with twitter access keys and tokens.')
    parser.add_argument('--loglevel', default='info',
                        choices=['info', 'debug', 'warn'],
                        help='The logging level for module logging.')
    parser.add_argument('--db_file', default='/data/search_stats.db',
                        help='SQLite3 file to use for statistics.')
    parser.add_argument('--output_dir', default='/data/tweets',
                        help='Folder for storing downloaded tweets.')
    parser.add_argument('--keywords_file', default='/data/keywords.txt',
                        help='Load keywords from a file.')
    args = parser.parse_args()

    # Import twitter keys as variables from .py file.
    # Default twitter_keys.py.
    try:
        module_dir, module_file = os.path.split(args.keys)
        module_name, module_ext = os.path.splitext(module_file)
        sys.path.append(module_dir)
        # sys.path.append('/data')
        twitter_keys = __import__(module_name)
        twitter_keys.consumer_key
        twitter_keys.consumer_secret
        twitter_keys.access_token
        twitter_keys.access_token_secret
    except Exception as e:
        logging.critical('Check twitter key file.')
        logging.critical(str(e))
        sys.exit()

    # logging setup
    if args.loglevel == 'info':    loglevel = logging.INFO
    if args.loglevel == 'debug':   loglevel = logging.DEBUG
    if args.loglevel == 'warning': loglevel = logging.WARNING
    logformat = '%(asctime)s %(levelname)10s %(message)s'
    datefmt = "%Y-%m-%d %I:%M:%S"
    logging.basicConfig(level=loglevel, format=logformat, datefmt=datefmt)

    # check if all tables are created in the db file
    helpers.check_db(args.db_file)


    # 
    # Main loop
    # 
    counter = 1
    while True:
        logging.info('Started cycle {:d}'.format(counter))
        try:
            twitter_search(db_file=args.db_file, 
                           output_dir=args.output_dir,
                           keywords_file=args.keywords_file)
        except TwitterSearch.TwitterSearchException as e:
            logging.warn('TwitterSearchException')
            logging.warn(str(e))
            logging.warn('Pausing for 15 min.')
            pause.minutes(15)
        except Exception as e:
            logging.warn('Something unexpected happened.')
            logging.warn(str(e))
            pause.minutes(5)
        counter += 1
        logging.info('Pausing for 3 min, safe to terminate.')
        logging.info('ctrl+C')
        try:
            pause.minutes(3)
        except:
            logging.info('Bye bye...')
            sys.exit()
        print()

