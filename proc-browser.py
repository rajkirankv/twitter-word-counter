from selenium import webdriver
import tweepy
from datetime import datetime, timedelta
import time
import csv

# parameters
verbose = False
step = 30
start_after = None


# 12/16/2016: return the substring between 2 given substrings
def find_between(s, first, last):
    try:
        start = s.index(first) + len(first)
        end = s.index(last, start)
        return s[start:end]
    except ValueError:
        return ""


# 12/16/2016: given a string, return the integer if the string represents an integer. Return 'none' otherwise
def str_to_int(s):
    try:
        return int(s)
    except ValueError:
        return None


# twitter API credentials
consumer_key = "2ftokUvglAwe4fO3kZfw5VaNM"
consumer_secret = "1OQSPZEZkcYM0UDdXItpv3buN4VCnAXx4DjeLl6JAiWcH6Gf6e"
access_key = "3142721558-1Km65GNsIwAEMFRUkxM7VneJovczwBjZNu3JcQk"
access_secret = "M1m9vcxU2K1T1CPIwX3tlrFLgq8LrtUzDaGebOZOMiJHR"

# authorize twitter, initialize tweepy
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_key, access_secret)
api = tweepy.API(auth)

# userlist
f_u = open('user-list.csv')
c_u = csv.reader(f_u)

next(c_u)

if start_after:
    while next(c_u)[0][1:] != start_after: pass

for entry in c_u:

    user = entry[0][1:]

    # use chromedriver (re-launch for each user)
    driver = webdriver.Chrome()
    driver.implicitly_wait(10)

    # output file
    f_o = open('proc-browser/' + user + '.csv', 'w', newline='')  # Updated the code for Python 3
    c_o = csv.writer(f_o, delimiter=',')

    c_o.writerow(['user', 'id', 'date', 'text', 'likes', 'retweets'])

    # get first tweet date
    if not entry[3]:
        while True:
            try:
                info = api.get_user(user)
            except tweepy.error.RateLimitError:
                print("limit exceeded, waiting 15 min...")
                time.sleep(15 * 60 + 30)
                continue
            break
        dft = info.created_at
    else:
        driver.get('https://discover.twitter.com/first-tweet#' + user)
        driver.find_element_by_id('twitter-widget-1')
        driver.switch_to_frame('twitter-widget-1')
        dft = driver.find_element_by_class_name('dt-updated').get_attribute('datetime')
        dft = datetime.strptime(dft.split('+')[0], '%Y-%m-%dT%H:%M:%S')

    # get last available tweet date (add 1 day for safety)
    f_a = open('proc-api/' + user + '.csv')
    c_a = csv.reader(f_a)
    for entry in c_a:
        last_entry = entry
    f_a.close()
    dlt = datetime.strptime(last_entry[2], '%Y-%m-%d %H:%M:%S')
    dlt += timedelta(days=1)

    if verbose:
        print('Processing: %s' % user)
        print(dft)
        print(dlt)

    total = 0
    since, until = dft, dft + timedelta(days=step)
    while since.date() < dlt.date():

        page = driver.get('https://twitter.com/search?f=tweets&q=from%3A' + user +
                          '%20since%3A' + since.strftime('%Y-%m-%d') +
                          '%20until%3A' + until.strftime('%Y-%m-%d') +
                          '%20include%3Aretweets' +
                          '%20include%3Areplies' +
                          '&src=typd')

        # scroll until we get all tweets (retry to ensure)
        i = 0
        tw_prev, tw_curr = 0, 1
        while i < 3:
            driver.execute_script('window.scrollTo(0, document.body.scrollHeight);')
            time.sleep(2)
            tweets = driver.find_elements_by_class_name('tweet')
            tw_prev = tw_curr
            tw_curr = len(tweets)
            if tw_curr == tw_prev:
                i += 1

        total += len(tweets) - 1

        # process each tweet
        p_tweets = []
        for tweet in tweets:

            # id (note: last on the list is None!)
            id = tweet.get_attribute('data-tweet-id')
            if not id:
                break

            # date (convert epoch to readable datetime)
            t_time = tweet.find_element_by_class_name('tweet-timestamp')
            t_time = t_time.get_attribute('title')
            try:
                t_time = datetime.strptime(t_time, '%I:%M %p - %d %b %Y')
                t_time = datetime.strftime(t_time, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                continue

            # text (encode in ascii ignoring unicode, and remove csv sep., \n, \r)
            text = tweet.find_element_by_class_name('tweet-text').text
            text = text.encode('ascii', 'ignore')
            text = text.replace(b',', b'').replace(b'\n', b'').replace(b'\r', b'').decode('ascii')

            # number of likes terenary operator
            likes = str_to_int(find_between(tweet.text, 'Like\n', 'More'))
            likes = (likes if isinstance(likes, int) else 0)

            # number of retweets terenary operator
            retweets = str_to_int(find_between(tweet.text, 'Retweet\n', 'Like'))
            retweets = (retweets if isinstance(retweets, int) else 0)

            if verbose:
                print('== TWEET ==============================================')
                print('ID: %s' % id)
                print('DATE: %s' % t_time)
                print('TEXT: %s' % text)

            p_tweets.append([user, id, t_time, text, likes, retweets])

        c_o.writerows(sorted(p_tweets, key=lambda x: x[1]))

        since = until
        until += timedelta(days=step)

    print('----------------------------------------')
    print('Processed: %s, %d tweets' % (user, total))
    print('----------------------------------------')

    f_o.close()
    driver.close();

f_u.close()
