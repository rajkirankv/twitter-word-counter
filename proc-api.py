import tweepy
import csv
import time

# twitter API credentials
consumer_key = "2ftokUvglAwe4fO3kZfw5VaNM"
consumer_secret = "1OQSPZEZkcYM0UDdXItpv3buN4VCnAXx4DjeLl6JAiWcH6Gf6e"
access_key = "3142721558-1Km65GNsIwAEMFRUkxM7VneJovczwBjZNu3JcQk"
access_secret = "M1m9vcxU2K1T1CPIwX3tlrFLgq8LrtUzDaGebOZOMiJHR"


def get_all_tweets(screen_name):
    # authorize twitter, initialize tweepy
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)
    api = tweepy.API(auth)

    alltweets = []

    # make initial request (200 is the maximum allowed count)
    while True:
        try:
            new_tweets = api.user_timeline(screen_name=screen_name, count=200)
        except tweepy.error.RateLimitError:
            print("limit exceeded, waiting 15 min...")
            time.sleep(15 * 60 + 30)
            continue
        break

    if (new_tweets):
        alltweets.extend(new_tweets)
        oldest = alltweets[-1].id - 1

    while len(new_tweets) > 0:

        while True:
            try:
                new_tweets = api.user_timeline(screen_name=screen_name, count=200, max_id=oldest)
            except tweepy.error.RateLimitError:
                print("limit exceeded, waiting 15 min...")
                time.sleep(15 * 60 + 30)
                continue
            break

        alltweets.extend(new_tweets)
        oldest = alltweets[-1].id - 1

    # outtweets = [[screen_name, tweet.id_str, tweet.created_at,
    #               tweet.text.encode("ascii", "ignore").replace(b',', b'').replace(b'\n', b'').replace(b'\r', b'')]
    #              for tweet in alltweets]
    # outtweets = [[screen_name, tweet.id_str, tweet.created_at, tweet.text.encode(encoding'utf-8', errors='ignore')
    # .replace(',', '').replace('\n', '').replace('\r', ''),tweet.favorite_count, tweet.retweet_count] for tweet in alltweets]
    outtweets = [[screen_name, tweet.id_str, tweet.created_at, tweet.text.encode(encoding='ascii', errors='ignore').
                replace(b',', b'').replace(b'\n', b'').replace(b'\r', b'').decode("ascii"), tweet.favorite_count,
                tweet.retweet_count] for tweet in alltweets]
    # #12/16/2016: Check if the file exists. If not, create the file
    # my_file = Path('proc-api/%s.csv' % screen_name)
    # if not my_file.is_file():

    with open('proc-api/%s.csv' % screen_name, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["user", "id", "date", "text", "likes", "retweets"])
        writer.writerows(outtweets)

    return len(outtweets)


if __name__ == '__main__':
    f_u = open('user-list.csv')
    c_u = csv.reader(f_u)

    next(c_u)

    for entry in c_u:
        user = entry[0][1:]
        total = get_all_tweets(user)
        print('Processed: %s, %d tweets' % (user, total))

    f_u.close()
