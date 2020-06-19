import os
import json
import glob
import pandas as pd
from multiprocessing import Process

def parse_articles(all_paths, articles_df_name= 'articles'):
    articles_df = pd.DataFrame(columns=('article_id', 'article_text', 'article_title','article_keywords', 'article_url', 'article_publish_date' ))
    article = {}
    for path in all_paths:
        article_id = path
        article_path = mypath + path

        news_path = article_path + '/news content.json'
        try:
            with open(news_path) as f:
                d = json.load(f)
        except:
            all_paths.remove(path)
            print("No content for ", path)
            pass

        article['article_id'] = path
        article['article_text'] = d['text']
        article['article_title'] = d['title']
        article['article_url'] = d['url']
        article['article_keywords'] = d['keywords']
        article['article_published_on'] = d['publish_date']
        articles_df = articles_df.append(article, ignore_index=True)    
        articles_df.to_csv('{}.csv'.format(articles_df_name))
    return(all_paths)

def parse_tweet(all_paths, user_df_name= 'user_df', tweet_df_name='tweet_df'):
    tweet_df = pd.DataFrame(columns=('article_id', 'tweet_id', 'tweet_text', 'tweet_created_at'))
    user_df = pd.DataFrame(columns=('tweet_id', 'user_id','user_name', 'user_location', 'user_follower_count', 'user_friend_count', 'user_favorite_count', 'user_statuses_count' ))

    tweet_dict = {}
    user_dict = {}

    for path in all_paths:
        article_id = path

        tweet_path = mypath + path + '/tweets/'
        all_tweets = glob.glob(tweet_path+"*.json")
        if all_tweets != []:
            for tweet in all_tweets:
                tweet_dict = {}
                try:
                    with open(tweet) as f:
                        d = json.load(f)
                except:
                    print("No tweet for", tweet_path)
                    pass

                tweet_dict['article_id'] = path
                tweet_dict['tweet_id'] = d['id']
                tweet_dict['tweet_text'] = d['text']
                tweet_dict['tweet_created_at'] = d['user']['created_at']

                user_dict['tweet_id'] = d['id']
                user_dict['user_id'] = d['user']['id']
                user_dict['user_name'] = d['user']['name']
                user_dict['user_location'] = d['user']['location']
                user_dict['user_follower_count'] =d['user']['followers_count']
                user_dict['user_friend_count'] = d['user']['friends_count']
                user_dict['user_favorite_count'] =d['user']['favourites_count']
                user_dict['user_statuses_count'] =d['user']['statuses_count']

                tweet_df = tweet_df.append(tweet_dict, ignore_index=True)
                user_df = user_df.append(user_dict, ignore_index=True)
            tweet_df.to_csv('{}.csv'.format(tweet_df_name))
            user_df.to_csv('{}.csv'.format(user_df_name))

def chunkify(lst,n):
    return [lst[i::n] for i in range(n)]
    
if __name__ == "__main__":
    mypath = "real/"
    all_paths = next(os.walk(mypath))[1]
    
    all_paths_new = parse_articles(all_paths)

    list_one, list_two, list_three, list_four, list_five, list_six = chunkify(all_paths_new,6)
    p1 = Process(target=parse_tweet(list_one, 'user_df_1', 'tweet_df_1'))
    p1.start()
    p2 = Process(target=parse_tweet(list_two, 'user_df_2', 'tweet_df_2'))
    p2.start()
    p3 = Process(target=parse_tweet(list_three, 'user_df_3', 'tweet_df_3'))
    p3.start()
    p4 = Process(target=parse_tweet(list_four, 'user_df_4', 'tweet_df_4'))
    p4.start()
    p5 = Process(target=parse_tweet(list_five, 'user_df_5', 'tweet_df_5'))
    p5.start()
    p6 = Process(target=parse_tweet(list_six, 'user_df_6', 'tweet_df_6'))
    p6.start()
