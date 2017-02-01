from selenium import webdriver
import tweepy
from datetime import datetime
import time
import csv

# twitter API credentials
consumer_key = "2ftokUvglAwe4fO3kZfw5VaNM"
consumer_secret = "1OQSPZEZkcYM0UDdXItpv3buN4VCnAXx4DjeLl6JAiWcH6Gf6e"
access_key = "3142721558-1Km65GNsIwAEMFRUkxM7VneJovczwBjZNu3JcQk"
access_secret = "M1m9vcxU2K1T1CPIwX3tlrFLgq8LrtUzDaGebOZOMiJHR"

# authorize twitter, initialize tweepy
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_key, access_secret)
api = tweepy.API(auth)

# open browser
driver = webdriver.Chrome()
driver.implicitly_wait(10)

f_o = open('proc-info/proc-info.csv', 'w', newline='')
c_o = csv.writer(f_o)

c_o.writerow(['user', 'name', 'verified', 'tweets', 'followers', 'following',
              'likes', 'location', 'website', 'joined', 'pv_count', 'profile'])

f_u = open('user-list.csv')
c_u = csv.reader(f_u)

next(c_u)

for entry in c_u:
    user = entry[0][1:]

    while True:
        try:
            info = api.get_user(user)
        except tweepy.error.RateLimitError:
            print("limit exceeded, waiting 15 min...")
            time.sleep(15 * 60 + 30)
            continue
        break

    # extract number of photos and videos
    driver.get('https://twitter.com/' + user)
    time.sleep(2)
    pv_count = driver.find_element_by_class_name('PhotoRail-headingWithCount')
    pv_count = pv_count.get_attribute('innerHTML').strip().encode('ascii')
    pv_count = pv_count.split()[0].replace(b',', b'')

    # some preprocess...
    name = info.name.encode('ascii', 'ignore')
    try:
        url = info.entities['url']['urls'][0]['expanded_url']
    except KeyError:
        url = ''
    date = datetime.strftime(info.created_at, '%Y-%m-%d %H:%M:%S')
    description = info.description.encode('ascii', 'ignore')
    description = description.replace(b',', b'').replace(b'\n', b'').replace(b'\r', b'')

    c_o.writerow([info.screen_name, name.decode('ascii'),
                  '1' if info.verified else '0', info.statuses_count,
                  info.followers_count, info.friends_count,
                  info.favourites_count, info.location,
                  url, info.created_at, pv_count.decode('ascii'), description.decode('ascii')])

driver.close()
f_o.close()
f_u.close()
