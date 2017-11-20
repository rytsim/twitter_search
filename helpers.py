import sys
import sqlite3
import logging
import requests
import os
import time
import bz2
import json
import pause
import pandas as pd
import TwitterSearch 
from urllib.parse import parse_qs, quote_plus, unquote 
from collections import defaultdict
from lxml import html





def check_db(db_file):
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    sql_commands =[
        '''
        CREATE TABLE IF NOT EXISTS latest_search
             (keyword text PRIMARY KEY,
              count int,
              min_date text,
              max_date text,
              max_id int,
              search_date text
              )
        ''',
        '''
        CREATE TABLE IF NOT EXISTS searches
             (keyword text,
              count int,
              min_date text,
              max_date text,
              max_id int,
              search_date text
              )
        ''',
        '''
        CREATE TABLE IF NOT EXISTS keywords
             (keyword UNIQUE
              )
        ''',
        '''
        CREATE TABLE IF NOT EXISTS iterations (
                start_time text,
                duration_min int,
                keywords int,
                tweets_got int,
                queries_submitted int,
                windows_used int)
        ''',
        '''
        CREATE TABLE IF NOT EXISTS totals
             (keyword text PRIMARY KEY,
              count int,
              min_date text,
              max_date text,
              last_search text
              )
        ''',
        '''
        CREATE TRIGGER IF NOT EXISTS update_totals_on_new_search AFTER INSERT ON latest_search
            BEGIN
                INSERT OR REPLACE INTO totals (keyword, count, min_date, max_date, last_search) 
                VALUES (
                    NEW.keyword, 
                    IFNULL((SELECT count FROM totals WHERE keyword=NEW.keyword), 0) + NEW.count,
                    IFNULL((SELECT min_date FROM totals WHERE keyword=NEW.keyword), NEW.min_date),
                    IFNULL(NEW.max_date,(SELECT max_date FROM totals WHERE keyword=NEW.keyword)),
                    NEW.search_date
                    );
            END;
        ''',
        '''
        CREATE TRIGGER IF NOT EXISTS update_searches_on_new_search AFTER INSERT ON latest_search
            BEGIN
                INSERT INTO searches (keyword, count, min_date, max_date, max_id, search_date) 
                VALUES (
                    NEW.keyword, 
                    NEW.count,
                    NEW.min_date,
                    NEW.max_date,
                    NEW.max_id,
                    NEW.search_date
                    );
            END;
        ''',
        '''
        CREATE TABLE IF NOT EXISTS exp_averages(
            keyword text UNIQUE,
            count real,
            max_id int,
            search_date text
            )
        ''',
        '''
        CREATE TRIGGER IF NOT EXISTS update_exp_avg_on_new_search AFTER INSERT ON latest_search
            BEGIN
                INSERT OR REPLACE INTO exp_averages (keyword, count, max_id, search_date) 
                VALUES (
                    NEW.keyword, 
                    NEW.count*0.2+IFNULL((SELECT count FROM exp_averages WHERE keyword=NEW.keyword),0)*0.8,
                    NEW.max_id,
                    NEW.search_date
                    );
            END;
        ''',
    ]

    try:
        for command in sql_commands:
            c.execute(command)
        conn.commit()
    except Exception as e:
        logging.critical('Stat db error')
        logging.critical(str(e))
        sys.exit()
    finally:
        c.close()
        conn.close()


def dict_to_sqlite(input_dict, table, db_file):
    '''
    INSERT INTO OR REPLACE
    Each dict as a row.
    Skip if integrity check failes.
    '''
    # conver to a list
    input_dict = [input_dict, ] if type(input_dict) is not list else input_dict

    conn = sqlite3.connect(db_file)
    c = conn.cursor()

    for row in input_dict:
        keys = ','.join(row.keys())
        values = list(row.values())
        question_marks = ','.join(list('?'*len(row)))
        SQL = 'INSERT OR REPLACE INTO '+table+' ('+keys+') VALUES ('+question_marks+')'
        try:
            c.execute(SQL, values)
        except sqlite3.IntegrityError:
            conn.rollback()
        except Exception as e:
            conn.rollback()
            logging.warn('db error')
            logging.warn(str(e))
        conn.commit()
    c.close()
    conn.close()


def get_tickers_nf(markets=['O', 'N', 'A']):
    '''
    Scrape tickers from selected markets, from netfonds.
    Expected input: market tickers ex.: ST, OSE ...
    Expected output: list of all tickers in specified markets
    '''
    markets = [markets, ] if type(markets) is not list else markets
    logging.debug('Getting tickers for %s markets.', len(markets))
    market_url = 'http://www.netfonds.no/quotes/kurs.php?exchange='
    tickers_xpath = "//div[@class='hcontent']//tr/td[1]/a/@href"
    tickers = []
    for market in markets:
        page_source = requests.get(market_url+market)
        tree = html.fromstring(page_source.text)
        tickers_current = tree.xpath(tickers_xpath)
        tickers += [i.split('=')[1] for i in tickers_current]
    tickers = sorted(tickers)
    logging.debug('Tickers got: %s', len(tickers))
    return tickers


def get_keywords_sql(db_file, table='keywords'):
    with sqlite3.connect(db_file) as conn:
        c = conn.cursor()
        c.execute('SELECT * FROM %s' % table)
        fetched = c.fetchall()

        
        # first time case
        # if keywords are empty use netfonds to get tickers
        if len(fetched) == 0:
            logging.info('No keywords in DB, fetching Tickers from netfonds...')
            # blacklisted tickers conflict with twitter keywords
            black_listed = ['OR'] 
            fetched = get_tickers_nf()
            fetched = [i.split('.')[0] for i in fetched]
            fetched = ['$'+i for i in fetched if i not in black_listed]
            # insert in to keywords for next use
            fetched = [(i,) for i in fetched]
            try:
                c.executemany('INSERT INTO '+table+' VALUES (?)', fetched)
            except:
                conn.rollback()
            conn.commit()
        c.close()
    return [i[0] for i in fetched]
    

def write_tweets(tweets, output_dir='tweets'):
    '''
    Writes all tweets to a file of current date.
    '''
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    file_name = 'tweets_'+time.strftime('%Y%m%d')+'.json.bz2'
    full_name = os.path.join(output_dir,file_name)
    with bz2.open(full_name, 'at') as f:
        for tweet in tweets:
            json.dump(tweet, f)
            f.write('\n')


def get_keywords_file(keyword_file):
    with open(keyword_file, 'r') as f:
        lines = f.readlines()
    lines = [i.strip('\n') for i in lines]
    lines = [i for i in lines if i != '']
    return lines


def generate_tso(keywords, db_file):
    '''
    Generate tsos combining least frequet keywords.
    '''

    # Get info from db on keywords
    # (keyword, count, max_id)
    with sqlite3.connect(db_file) as conn:
        c = conn.cursor()
        c.execute('SELECT keyword, count, max_id FROM exp_averages')
        latest = c.fetchall()
        c.close()

    # mere keywords data with sql data
    latest_df = pd.DataFrame(latest, columns=['keyword', 'count', 'max_id'])
    df = pd.DataFrame(keywords, columns=['keyword'])
    df = df[df['keyword'] != '$OR'] # twitter keywords... not allowed
    df = df[df['keyword'] != 'OR']  # same
    df = pd.merge(df, latest_df, how='left', on='keyword')

    thresholds = [
        {'count':    3,      'combine': 50},
        {'count':   10,      'combine': 10},
        {'count':   20,      'combine':  4},
        {'count':   40,      'combine':  2},
        {'count': None,      'combine':  1},
        ]

    for threshold in thresholds:
        # Select a section from the df
        # truncating the df as it goes through the thresholds
        # new tweets will have None as count and thus will not
        # meet any thresholds and will be processed one at time.
        if threshold['count']:
            section = df[df['count'] < threshold['count']]
        else:
            section = df[:]

        df.drop(section.index, inplace=True)

        # Generate tsos
        while len(section) > 0:
            # determine the right number of keywords to combine
            try_n = threshold['combine']
            too_long = True
            while too_long:
                subsection = section[:try_n]
                combine = list(subsection.keyword)
                
                # use the smallest of the max_id because in the time 
                # from min(max_id) to max(max_id) there might have been
                # tweets for keywords other then the one of max(max_id)
                max_id = subsection['max_id'].min()
                tso = TwitterSearch.TwitterSearchOrder()
                tso.set_include_entities(True)
                tso.set_result_type('recent')
                tso.set_keywords(combine, or_operator=True)
                if not pd.isnull(max_id):
                    tso.set_since_id(int(max_id))

                url = tso.create_search_url()
                
                if (len(url) < 450) | (try_n == 1):
                    # exit clause
                    too_long = False
                    logging.debug('Number of tickers combnied {}'.format(len(combine)))
                    logging.debug(combine)
                    logging.debug(tso.create_search_url())
                else:
                    try_n -= 1
                
            yield tso
            section = section.iloc[try_n:]

#debuging
# Setting the logging params for ipython 
#formater = logging.Formatter('%(asctime)s %(levelname)10s %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
#logger = logging.getLogger()
#logger.setLevel(logging.DEBUG)
#handler = logger.handlers[0]
#handler.setFormatter(formater)
#fh = logging.FileHandler('debuglog.log')
#fh.setLevel(logging.DEBUG)
#fh.setFormatter(formater)
#logger.addHandler(fh)
#logging.debug('test')
#
#    
#db_file = 'example.db'
#keywords = get_keywords_sql(db_file)
#tso_gen = generate_tso(keywords, db_file)
#for i in tso_gen:
#    pass


def submit_tso(tso, ts, output_dir):
    # get params from tso object
    url = tso.create_search_url()
    tso_params = parse_qs(url)
    since_id = tso_params.get('since_id', None)
    since_id = since_id if since_id is None else since_id[0] 
    keywords = tso_params.get('?q', None)
    if keywords:
        keywords = set(keywords[0].split(' '))
        keywords = [kw.strip('"') for kw in keywords if kw != 'OR']
        keywords = set(keywords) #just in case stripinus '"' atsirado duplikatu

    # use defaultdict(list) as each tso might have many pages
    # append all pages to a list and aggregate after
    max_id   = defaultdict(list)
    max_date = defaultdict(list)
    min_date = defaultdict(list) 
    count    = defaultdict(list)
    window_count = 0

    ts.search_tweets(tso)
    try_next = True
    while try_next:     
        # parse response
        meta = ts.get_metadata()
        remaining_limit = int(meta.get('x-rate-limit-remaining',0))            
        num_tweets = ts.get_amount_of_tweets()
    
        # process tweets if there are any
        if num_tweets != 0:
            tweets = ts.get_tweets().get('statuses', [])
            write_tweets(tweets, output_dir)
            # for now only with cashtags
            # todo: hashtags and simple keywords..
            current_max_id = max([t['id'] for t in tweets]) # max id off all
            for kw in keywords:
                kw_tweets = [t for t in tweets if kw in ['$'+i['text'] for i in t['entities']['symbols']]]
                max_id[kw].append(current_max_id) # max id off all tso
                if len(kw_tweets) != 0:
                    max_date[kw].append(max([pd.to_datetime(tweet['created_at'], utc=True) for tweet in kw_tweets]))
                    min_date[kw].append(min([pd.to_datetime(tweet['created_at'], utc=True) for tweet in kw_tweets]))
                    count[kw].append(len(kw_tweets))
        
        if remaining_limit == 0:
            try:
                limit_reset = int(meta.get('x-rate-limit-reset', time.time()+15*60)) + 10 # extra sec to be on the safe side
                # convert to correct datetime
                limit_reset_dt = pd.to_datetime(limit_reset, unit='s', utc=True)
                limit_reset_dt = limit_reset_dt.tz_convert('Europe/London')
                logging.debug('Sleeping until {:%H:%M:%S}'.format(limit_reset_dt))
                pause.until(limit_reset)
            except Exception as e:
                logging.warn('limit_reset ERROR')
                logging.warn(str(e))
                logging.warn('Sleep for 15min...')
                # wait the maximum time until next window...
                pause.minutes(15)
            window_count += 1
        # check if there is a next page for the tso
        try:
            try_next = ts.search_next_results()
        except:
            try_next = False

    # aggregate stats for current tso
    tso_stats = []
    for kw in keywords:
        max_id[kw] = max(max_id[kw]) if len(max_id[kw]) !=0 else since_id#??
        max_date[kw] = max(max_date[kw]) if len(max_date[kw]) !=0 else None
        min_date[kw] = min(min_date[kw]) if len(min_date[kw]) !=0 else None
        count[kw] = sum(count[kw])

        
        tso_stats.append({
            'keyword':kw,
            'count':count[kw],
            'min_date': min_date[kw].strftime('%Y-%m-%d %H:%M:%S') if not min_date[kw] is None else None,
            'max_date': max_date[kw].strftime('%Y-%m-%d %H:%M:%S') if not max_date[kw] is None else None,
            'max_id': max_id[kw],
            'search_date': pd.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            })
            
    return tso_stats, window_count

