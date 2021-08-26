import requests
import pandas as pd
from datetime import datetime
import time

# This script uses the pushshift API to pull all comments and posts from R/GME in Jan-Jun 2021

# we use this function to convert responses to dataframes
def df_from_response(res):
    '''Processes response object from API into DF'''
    # initialize temp dataframe for batch of data in response
    df = pd.DataFrame()

    # loop through each post pulled from res and append to df
    for post in res.json()['data']:
        try:
            df = df.append({
                'subreddit': post['subreddit'],
                'title': post['title'],
                'selftext': post['selftext'],
                'upvote_ratio': post['upvote_ratio'],
                'created_utc': datetime.fromtimestamp(post['created_utc']).strftime('%Y-%m-%dT%H:%M:%SZ'),
                'utc_raw': post['created_utc'],
                'id': post['id'],
                'kind': 't3'
            }, ignore_index=True)
        #catch missing self_text
        except:
            df = df.append({
                'subreddit': post['subreddit'],
                'title': post['title'],
                'selftext': '',
                'upvote_ratio': post['upvote_ratio'],
                'created_utc': datetime.fromtimestamp(post['created_utc']).strftime('%Y-%m-%dT%H:%M:%SZ'),
                'utc_raw': post['created_utc'],
                'id': post['id'],
                'kind': 't3'
            }, ignore_index=True)

    return df

# get all posts from June on back to Jan 1 2021
before = 1625115600
data = pd.DataFrame()
newest_month = 0

while before > 1609480800:
    time.sleep(3)
    # make request
    res = requests.get(f"https://api.pushshift.io/reddit/search/submission/?subreddit=gme&sort=desc&sort_type=created_utc&after=1609480800&before={before}&size=100")
    # check for empty set
    if res.text == '{\n    "data": []\n}':
        break
    # get dataframe from response
    new_df = df_from_response(res)
    # take the final row (oldest entry)
    row = new_df.iloc[len(new_df)-1]
    # get earliest date
    before = int(row['utc_raw'])
    # show where we're at
    if datetime.fromtimestamp(before).strftime('%m') != newest_month:
        newest_month = datetime.fromtimestamp(before).strftime('%m')
        print(f"starting on month {datetime.fromtimestamp(before).strftime('%m')}.")
    
    # append new_df to data
    data = data.append(new_df, ignore_index=True)

### COMMENTS ###

# we use this function to convert responses to dataframes
def df_from_comment(res):
    '''Processes Response object into DF'''
    # initialize temp dataframe for batch of data in response
    df = pd.DataFrame()

    # loop through each post pulled from res and append to df
    for post in res.json()['data']:
        try:
            df = df.append({
                'subreddit': post['subreddit'],
                'parent': post['parent_id'],
                'body': post['body'],
                'created_utc': datetime.fromtimestamp(post['created_utc']).strftime('%Y-%m-%dT%H:%M:%SZ'),
                'utc_raw': post['created_utc'],
                'id': post['id'],
                'kind': 't2'
            }, ignore_index=True)
        #catch missing body
        except:
            df = df.append({
                'subreddit': post['subreddit'],
                'parent': post['parent_id'],
                'body': '',
                'created_utc': datetime.fromtimestamp(post['created_utc']).strftime('%Y-%m-%dT%H:%M:%SZ'),
                'utc_raw': post['created_utc'],
                'id': post['id'],
                'kind': 't2'
            }, ignore_index=True)

    return df

def get_comments(start, end):
    '''Gets all comments between two UTC epochs
    start (int) - UTC epoch for start of period
    end (int) - UTC epoch for end of period'''
    # get all posts from start to end
    before = end
    comments = pd.DataFrame()
    newest_month = 0
    tot_comments = 0
    while before > start:
        # make request
        time.sleep(3)
        res = requests.get(f"https://api.pushshift.io/reddit/search/comment/?subreddit=gme&sort=desc&sort_type=created_utc&after=1609480800&before={before}&size=500")
        if res.ok == False and res.status_code >= 500:
            continue
        if res.ok == False and res.status_code < 500:
            print("too many requests")
            break
        # check for empty set
        if res.text == '{\n    "data": []\n}':
            break
        # get dataframe from response
        new_df = df_from_comment(res)
        # take the final row (oldest entry)
        row = new_df.iloc[len(new_df)-1]
        # get earliest date
        before = int(row['utc_raw'])
        # show where we're at
        if datetime.fromtimestamp(before).strftime('%m') != newest_month:
            newest_month = datetime.fromtimestamp(before).strftime('%m')
            print(f"starting on month {datetime.fromtimestamp(before).strftime('%m')}.")
        tot_comments += 500
        if tot_comments % 10000 == 0 and tot_comments > 0:
                print(f'{tot_comments} processed so far.')
        
        # append new_df to data
        comments = comments.append(new_df, ignore_index=True)

jun_comments = get_comments(1622523600,1625115600)
may_comments = get_comments(1619845200,1622523600)
apr_comments = get_comments(1617253200,1619845200)
mar_comments = get_comments(1614578400,1617253200)
feb_comments = get_comments(1612159200,1614578400)
jan_comments = get_comments(1609480800,1612159200)

june_comments.to_csv(r"C:\Users\Daniel\Documents\NLP Class\Project\gme_comments_jun.csv")
may_comments.to_csv(r"C:\Users\Daniel\Documents\NLP Class\Project\gme_comments_may.csv")
apr_comments.to_csv(r"C:\Users\Daniel\Documents\NLP Class\Project\gme_comments_apr.csv")
mar_comments.to_csv(r"C:\Users\Daniel\Documents\NLP Class\Project\gme_comments_mar.csv")
feb_comments.to_csv(r"C:\Users\Daniel\Documents\NLP Class\Project\gme_comments_feb.csv")
jan_comments.to_csv(r"C:\Users\Daniel\Documents\NLP Class\Project\gme_comments_jan.csv")