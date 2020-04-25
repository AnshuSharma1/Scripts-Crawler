import time
from urllib.parse import urlencode

import requests


class PratilipiSpider:
    base_url = 'https://hindi.pratilipi.com/'
    params = {
        'language': 'HINDI',
        'offset': 0,
        'category': '',
        'fromSec': 300,
        'toSec': 1799
    }

    headers = {
        "Cookie": ""
    }

    def __init__(self, language):
        self.session = requests.Session()
        self.params['language'] = language

    def get_sorted_articles(self, category, url, retry_count):
        retry = 0
        offset = 0
        articles = []
        display_titles = []
        params = dict(self.params)
        params['category'] = category

        while retry < retry_count:
            response = requests.get(url + urlencode(params), headers=self.headers)
            while response.status_code != 404:
                data = response.json()
                if data['pratilipiList']:
                    for article in data['pratilipiList']:
                        if article['displayTitle'] not in display_titles:
                            display_titles.append(article['displayTitle'])
                            articles.append(article)

                    offset += 20
                    params.update({
                        'offset': offset
                    })
                    next_url = url + urlencode(params)
                    response = self.session.get(next_url, headers=self.headers)
                else:
                    break
            retry += 1
            time.sleep(3)

        return articles

    def get_popular_articles(self):
        high_rated_url = 'https://hindi.pratilipi.com/api/stats/v2.0/high_rated?'
        articles = self.get_sorted_articles('lovestories', high_rated_url, 1)
        print('POPULAR', len(articles))

        return articles

    def get_recent_articles(self):
        recent_url = 'https://hindi.pratilipi.com/api/stats/v2.0/recent_published?'
        articles = self.get_sorted_articles('lovestories', recent_url, 3)
        print('RECENT', len(articles))

        return articles

    def __str__(self):
        return 'Pratilipi Spider'


if __name__ == "__main__":
    crawler = PratilipiSpider('HINDI')
