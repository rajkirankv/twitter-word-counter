#from importlib import reload
import numpy as np
import pandas as pd
import os
import csv
import regex as re
from pandas import DataFrame, Series


# Remove links and company names given a tweet and the handle that tweeeted it
def clean_tweet(tweet_copy, twitter_handle):
    names_file_name = 'management_company_names.csv'
    synonyms_data = pd.read_csv(names_file_name, header=0, encoding='latin1')  # read the company names input into the data frame
    synonyms_data.fillna(value='', inplace=True)  # takes care of blank cells
    synonyms_data.drop(labels=synonyms_data.columns[0], axis=1, inplace=True)  # I would directly use the column name
    # 'userid' to remove it but it just won't work. A space char is prefixed in the column name somehow
    synonyms_data['username'] = synonyms_data['username'].str.replace('@', '', 1)  # remove the twitter twitter_handle symbol @
    synonyms = synonyms_data[synonyms_data['username'] == twitter_handle].squeeze()  # reduce a 1 row data frame into a series
    synonyms = synonyms.str.lower()  # convert everything to lower case for comparision purposes

    # remove the parts mentioned in the input file
    # pointer_to_the_original_tweet = tweet_copy  # this is necessary because strings are immutable
    for synonym in synonyms.values:
        tweet_copy = tweet_copy.replace(synonym, '')

    # remove the keywords occuring as a part of the url
    url_instance = re.compile(r'http.+\s|(http.+)$', re.IGNORECASE)
    while re.search(url_instance, tweet_copy) is not None:
        tweet_copy = re.sub(url_instance, '', tweet_copy)

    return tweet_copy


# given the set of tweets by a given handle and a list of variations, return the table of occurances with tweet id
# along the rows and variations along the columns
def count_tweets(tweets, keywords):
    count_table = DataFrame(data=None, index=tweets['id'], columns=keywords)

    for tweet_id, tweet in tweets[['id', 'text']].values:
        tweet = clean_tweet(tweet, tweets.name)
        for var in keywords.values:
            count = len(re.findall(var + r'[^a-zA-Z]', tweet, re.IGNORECASE))
            count_table.ix[tweet_id, [var]] = count

    return count_table


# Read category --> key words --> variations dictionary data
dict_df = pd.read_csv('dictionary.csv', header=0)  # Dict data read into a data frame
dict_df.apply(lambda x: x.astype(str).str.lower())
# Convert the data frame into a multiindex
df_mi = pd.MultiIndex.from_arrays(dict_df.values.T, names=['category', 'keywords', 'variation'])

# Read all the twitter handles
user_list_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'user-list.csv')
handles = pd.read_csv(user_list_path, header=0)  # This will be 1D list of lists

# Dataframes in which the results are stored
variation_level_table = None
keyword_level_table = None
category_level_table = None

# Given a filename with merged data in the proc-merge folder, read the tweets, count the occurances and save the output
# to a file
for handle in handles.values:
    # For each handle and it's corresponding file, read the tweets data
    tweets_file_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'proc-merge')  # Set file path
    file_name = handle[0][1:] + '.csv'  # For each handle, set file name
    tweets_file_path = os.path.join(tweets_file_path, file_name)
    tweets_per_handle = pd.read_csv(tweets_file_path, header=0)  # The headers of columns should not change
    tweets_per_handle.index = tweets_per_handle['id']  # make the tweet id this dataframe's index for joins later on
    tweets_per_handle['text'] = tweets_per_handle['text'].str.lower()  # convert everything to lower case
    tweets_per_handle.name = file_name[:-4]  # Name of the account

    # For each tweet, count the occurance of each variation
    tweet_count = count_tweets(tweets_per_handle, df_mi.levels[2])

    # Aggregate the occarance count of variations into the occurance count of categories
    tweet_count.columns = df_mi  # This is a data frame with tweet id as row index and the multiindex as column index
    category_level_sum = tweet_count.groupby(level=['category'], axis=1).sum()
    column_order = tweets_per_handle.columns.values.tolist() + category_level_sum.columns.values.tolist()
    tweets_per_handle = pd.concat([tweets_per_handle, category_level_sum], axis=1, join='inner')
    tweets_per_handle = tweets_per_handle[column_order]

    # Save the output to a file in the dictionaries folder with the same name as the handle
    tweets_per_handle.to_csv(file_name)

    print('----------------------------------------')
    print('Word count for user %s with %d tweets is saved in %s' % (handle[0][1:], tweet_count.shape[0], file_name))
    print('----------------------------------------')

print('Word counts saved successfully')
