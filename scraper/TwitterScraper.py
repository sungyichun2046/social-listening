from __future__ import absolute_import
import sys 
sys.path.append("/home/yichun/projects/social-listening")
from scraper.CIV_Scraper import CIV_Scraper
import twint
"""
TO DO: 
 1 location name
 2 get replies of tweets
 3 complete user data: sex(can find it by name), age(can find it by picture)
"""

class TwitterScraper(CIV_Scraper):
    def __init__(self, search_term=None, lang=None, since=None, until=None):
        self.search_term = search_term
        self.lang = lang
        self.since = since
        self.until = until

    def get_data(self):
        c = twint.Config()
        #c.Username = "LCL"
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
        #tweets_df= twint.storage.panda.Tweets_df
        df_pd = twint.output.panda.Tweets_df[["username","tweet", "hashtags", "place", "date", "nlikes", "link"]]

        """
        #get user data
        ser = pd.Series(df_pd["username"])
        username = ser[0]
        print(self.get_user_data(username=username))
        """
        return df_pd

    def get_user_data(self, username=None):
        c = twint.Config()
        c.Username = username
        c.Store_object = True
        c.Pandas = True
        twint.run.Lookup(c)
        user_df = twint.storage.panda.User_df[["name","username", "id", "likes", "followers", "following" ,"location"]]
        print (user_df.to_string(index=False))

    def convert_pandas_to_spark(self, df_pd):
        pass

test = TwitterScraper(search_term="LCL banque", lang="fr", since="2018-12-01", until="2018-12-02")
df_pd = test.get_data()
print (df_pd.to_string(index=False))
