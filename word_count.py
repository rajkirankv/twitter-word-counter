from importlib import reload
import numpy as np
import pandas as pd
import os
import csv
from pandas import DataFrame, Series

def count_tweets(tweets, keywords):
    count_table = DataFrame(data=None, index=tweets['id'], columns=keywords)
    for tweet_id, tweet in tweets[['id', 'text']].values:
        for var in keywords:
            count = 0  # Number of times var occured in tweet
            var_length = len(var)  # Length of the word
            position = tweet.find(var, 0)  # Find where the word in var is located in the tweet if any
            while position != -1:
                count += 1
                position = tweet.find(var, position + var_length)
            count_table.ix[tweet_id, [var]] = count
    return count_table


# Read dictionary data. This will be a multiIndex
dict_df = pd.read_csv('dictionary.csv', header=0)  # Dict data read into a data frame
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
    tweets_per_handle = pd.read_csv(tweets_file_path + file_name, header=0,
                                    usecols=['id', 'text'])  # The headers of columns should not change
    tweets_per_handle.name = file_name[:-4]  # Name of the account

    # For each tweet, count the occurence of each variation
    tweet_count = count_tweets(tweets_per_handle, df_mi.levels[2])  # Get variation appearance count per tweet
    tweet_count.columns = df_mi  # This is a data frame with tweet id as row index and the multiindex as column index

    # Agrregate the occurences across tweets at various levels
    variation_level_sum = tweet_count.sum(axis=0)
    keyword_level_sum = variation_level_sum.sum(axis=0, level=['keywords'])
    category_level_sum = variation_level_sum.sum(axis=0, level=['category'])

    # Prepare the output tables
    variation_level_sum.index = variation_level_sum.index.levels[2]
    variation_level_sum.name = handle[0][1:]
    variation_level_table = pd.concat((variation_level_table, variation_level_sum), axis=1, join='outer')

    keyword_level_sum.name = handle[0][1:]
    keyword_level_table = pd.concat((keyword_level_table, keyword_level_sum), axis=1, join='outer')

    category_level_sum.name = handle[0][1:]
    category_level_table = pd.concat((category_level_table, category_level_sum), axis=1, join='outer')

#Save the data to csv
variation_level_table.T.to_csv("variation_level_table.csv")
keyword_level_table.T.to_csv("keyword_level_table.csv")
category_level_table.T.to_csv("category_level_table.csv")