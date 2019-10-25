from __future__ import absolute_import
import sys 
sys.path.append("/home/yichun/projects/social-listening")
from scraper.CIV_Scraper import CIV_Scraper
import os
import twint
from datetime import datetime, timedelta
import logging
logger = logging.getLogger('ftpuploader')
"""
TO DO:
 1 Speed up function get_user_location (https://stackoverflow.com/questions/33518124/how-to-apply-a-function-on-every-row-on-a-dataframe)  
 2 get replies of tweets
 3 complete user data: sex(can find it by name), age(can find it by picture)

USAGE:
    term = "LCL"
    test = TwitterScraper(search_term=term, lang="fr", since="2019-09-01", until="2019-09-30")
    df_pd = test.get_data()
    if not os.path.exists("datas/2019-09"):
        os.makedirs("datas/2019-09")
    test.store_pandas_df(df=df_pd, file_name="datas/{}/{}.csv".format("2019-09", term))
"""

class TwitterScraper(CIV_Scraper):
    def __init__(self, search_term=None, lang=None, since=None, until=None):
        self.search_term = search_term
        self.lang = lang
        self.since = since
        self.until = until

    def twint_to_pandas(self,columns):
        return twint.output.panda.Tweets_df[columns]

    def get_data(self):
        c = twint.Config()
        c.Since = self.since
        c.Until = self.until
        c.Lang = self.lang
        c.Search = self.search_term
        c.Location = True
        c.Show_hashtags = True
        c.Count = True
        c.Stats = True
        c.Debug = True
        c.Replies = True
        c.Pandas_clean = True
        c.Get_replies = True
        c.Pandas = True
        twint.run.Search(c)
        df_pd = twint.output.panda.Tweets_df[["id","username","tweet", "hashtags", "date", "nlikes", "link"]]
        #Add user location column : apply function get_users_location on every row on dataframe
        df_pd['location'] = df_pd.apply(lambda row: self.get_user_location(username=row['username']), axis=1)
        return df_pd

    def get_user_location(self, username=None):
        location = None
        c = twint.Config()
        c.Username = username
        c.Store_object = True
        c.Pandas = True
        try:
            twint.run.Lookup(c)
            users = twint.storage.panda.User_df[["name","username", "id", "likes", "followers", "following" ,"location"]]
            user = users.loc[users['username'] == username]
            location = list(user['location'])[0]
        except Exception as e:
            logger.error('Failed to find location: '+ str(e))
        return location

    def convert_pandas_to_spark(self, df_pd):
        pass

def get_data_for_one_month(since=None, until=None, search_term=None):
    """
    Store one dataframe per day
    :param since: start date we scrap
    :param until: until date we scrap
    :param search_term: search term
    """
    while since<until:
        since = since
        date = datetime.strptime(since, "%Y/%m/%d")
        until = date + timedelta(days=1)
        until = datetime.strftime(until, "%Y/%m/%d").replace("/", "-")
        since = since.replace("/", "-")
        test = TwitterScraper(search_term=search_term, lang="fr", since=since, until=until)
        date +=1
        test.store_pandas_df(df=df_pd, file_name="datas/{}/{}_test.csv".format(since, term))


