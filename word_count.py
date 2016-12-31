from importlib import reload
import numpy as np
import pandas as pd
import os
import csv
import regex as re
from pandas import DataFrame, Series


# Remove links and company names given a tweet and the handle that tweeeted it
def clean_tweet(tweet, handle):
    if tweet == 'we are passionate about our optimized low volatility investment strategy. find out more about it here: http://summitglobalinvestments.com/investment_strategy.html ':
        print()
    file_name = 'management_company_names.csv'
    synonyms_data = pd.read_csv(file_name, header=0)  # read the company names input into the data frame
    synonyms_data.fillna(value='', inplace=True)
    synonyms_data.drop(labels=synonyms_data.columns[0], axis=1, inplace=True)  # I would directly use the column name
    # 'userid' to remove it but it just won't work. A space char is prefixed in the column name somehow
    synonyms_data['username'] = synonyms_data['username'].str.replace('@', '', 1)  # remove the twitter handle symbol @
    synonyms = synonyms_data[synonyms_data['username'] == handle].squeeze()

    # remove the parts mentioned in the input file
    for synonym in synonyms.values:
        clipped_tweet = tweet.replace(synonym, '')

    # remove the keywords occuring as a part of the url
    url_instance = re.compile(r'http.+\s|(http.+)$', re.IGNORECASE)
    clipped_tweet = url_instance.sub('', clipped_tweet)

    return clipped_tweet


# delete text corresponding to any shared links within the tweet
# delete part of the text that constitutes the company name


def count_tweets(tweets, keywords):
    # Convert everything to lower case
    # tweets = tweets.lower()

    count_table = DataFrame(data=None, index=tweets['id'], columns=keywords)

    for tweet_id, tweet in tweets[['id', 'text']].values:
        tweet = clean_tweet(tweet, tweets.name)
        for var in keywords.values:
            if var in ['active',
'advisor',
'advisors',
'adviser',
'advisers',
'etf',
'etfs',
'invest',
'investable',
'invested',
'investment',
'investible',
'investing',
'investments',
'invests',
'investings',
'investor',
'investors',
'investers',
'ipo',
'ipos',
'portfolio',
'portfolios',
'retirement',
'retirements',
'retire',
'retired',
'retires',
'retiring']:
                print()
            count = 0  # Number of times var occured in tweet
            var_length = len(var)  # Length of the word
            position = tweet.find(var, 0)  # Find where the word in var is located in the tweet if any
            while position != -1:
                var_regex = re.compile(r'(invest[^a-zA-Z])')
                count += 1
                position = tweet.find(var, position + var_length)
            count_table.ix[tweet_id, [var]] = count

    return count_table


# Read dictionary data. This will be a multiIndex
dict_df = pd.read_csv('dictionary.csv', header=0)  # Dict data read into a data frame
# dict_df = dict_df.str.lower() # Convert all strings to lower case for case insensitive comparison
dict_df.apply(lambda x: x.astype(str).str.lower())
# Convert the data frame into a multiindex
df_mi = pd.MultiIndex.from_arrays(dict_df.values.T, names=['category', 'keywords', 'variation'])
# Read all the twitter handles
user_list_path = os.path.dirname(os.path.dirname(__file__)) + '/user-list.csv'
handles = pd.read_csv(user_list_path, header=0)  # This will be 1D list of lists

# Dataframes in which the results are stored
variation_level_table = None
keyword_level_table = None
category_level_table = None

# Given a filename with merged data, read the tweets
for handle in handles.values:
    # Read the tweets of the given handle
    tweets_file_path = os.path.dirname(os.path.dirname(__file__)) + '/proc-merge/'
    file_name = handle[0][1:] + '.csv'
    # tweets_per_handle = pd.read_csv(tweets_file_path + file_name, header=0,
    #                                 usecols=['id', 'text'])  # The headers of columns should not change
    tweets_per_handle = pd.read_csv(tweets_file_path + file_name, header=0)  # The headers of columns should not change
    tweets_per_handle.index = tweets_per_handle[
        'id']  # make the tweet id this dataframe's index. useful for comparision
    tweets_per_handle['text'] = tweets_per_handle['text'].str.lower()  # convert everything to lower case
    tweets_per_handle.name = file_name[:-4]  # Name of the account

    # For each tweet, count the occurance of each variation
    tweet_count = count_tweets(tweets_per_handle, df_mi.levels[2])  # Get variation appearance count per tweet

    # ----Word count per tweet
    tweet_count.columns = df_mi  # This is a data frame with tweet id as row index and the multiindex as column index
    category_level_sum = tweet_count.groupby(level=['category'], axis=1).sum()
    # tweets_per_handle = tweets_per_handle.append(category_level_sum)
    column_order = tweets_per_handle.columns.values.tolist() + category_level_sum.columns.values.tolist()
    tweets_per_handle = pd.concat([tweets_per_handle, category_level_sum], axis=1, join='inner')
    tweets_per_handle = tweets_per_handle[column_order]

    tweets_per_handle.to_csv(file_name)

    print()
