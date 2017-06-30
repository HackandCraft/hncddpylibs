import logging

import base64

log = logging.getLogger(__file__)

import requests


class TwitterException(Exception):
    pass


class RateLimitException(TwitterException):
    pass


class AccountSuspendedException(TwitterException):
    pass


def twitter_api_url(url):
    return 'https://api.twitter.com%s' % url


def get_tweets_page(screen_name, token, from_id=None):
    params = dict(screen_name=screen_name.strip('@'), count=200)
    if from_id:
        params['max_id'] = from_id - 1

    resp = requests.get(twitter_api_url('/1.1/statuses/user_timeline.json'),
                        params=params,
                        headers={'Authorization': 'Bearer {}'.format(token)})
    if resp.status_code == [420, 429]:
        log.warning('TWITTER_NETWORK_THROTTLED: %s, %s, %s', screen_name, resp.status_code, resp.content)
        raise RateLimitException('TWITTER LIMIT')
    elif resp.status_code == [401, 410]:
        log.warning('TWITTER_ACCOUNT_ERROR: %s, %s, %s', screen_name, resp.status_code, resp.content)
        raise AccountSuspendedException('TWITTER LIMIT')
    elif resp.status_code != 200:
        log.error('TWITTER_NETWORK_ERROR: %s, %s, %s', screen_name, resp.status_code, resp.content)
        raise TwitterException(resp.content)
    return resp.json()


def tweet_formatter(tweet):
    return {
        'Id': tweet['id'],
        'Text': tweet['text'],
        'Created': tweet['created_at'],
        'FavouriteCount': tweet['favorite_count'],
        'RetweetCount': tweet['retweet_count'],
        'Retweeted': tweet['retweeted']
    }


def get_fresh_token(creds):
    if 'token' in creds:
        return creds['token']
    keys = ('%s:%s' % (creds['Key'], creds['Secret'])).encode('utf-8')
    key = base64.b64encode(keys)
    auth_string = 'Basic {}'.format(key.decode('utf-8'))
    resp = requests.post(twitter_api_url('/oauth2/token'),
                         data=dict(grant_type='client_credentials'),
                         headers={'Authorization': auth_string})
    json_resp = resp.json()
    if not json_resp.get('access_token'):
        raise TwitterException('%s-%s' % (creds['key'], resp.content))
    return json_resp.get('access_token')


def get_all_tweets(creds, screen_name, limit, transform=tweet_formatter):
    token = get_fresh_token(creds)
    tweets = []
    from_id = None
    while True:
        try:
            page = get_tweets_page(screen_name, token, from_id)
        except TwitterException:
            return []
        if len(page) == 0:
            break
        tweets.extend([transform(tweet) for tweet in page])
        if len(tweets) >= limit:
            break
        last_tweet = page[-1]
        from_id = last_tweet['id'] - 1
    return tweets
