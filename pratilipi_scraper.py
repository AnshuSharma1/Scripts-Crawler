import requests


class PratilipiSpider:
    base_url = 'https://hindi.pratilipi.com/'

    def __init__(self):
        self.session = requests.Session()

    def get_trending_articles(self):
        pass

    def get_popular_articles(self):
        pass

    def get_recent_articles(self):
        pass

    def parse_articles(self, category):
        category_url = self.base_url + category
        response = self.session.get(category_url)

    def __str__(self):
        return 'Pratilipi Spider'


if __name__ == 'main':
    crawler = PratilipiSpider()
