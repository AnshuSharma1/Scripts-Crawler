import time
from datetime import datetime
from urllib.parse import urlencode

import requests

from constants import AUTHORS_INSERT_QUERY, SCRIPTS_INSERT_QUERY
from helpers import execute_query


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

    unique_ids = set()
    articles = []

    def __init__(self, language):
        self.session = requests.Session()
        self.params['language'] = language

    @staticmethod
    def get_datetime(unix_timestamp):
        return datetime.fromtimestamp(unix_timestamp / 1000)

    @staticmethod
    def get_latest_timestamp(recent):
        if recent:
            cursor = execute_query("""SELECT MAX(site_updated_at) FROM pratilipi_scripts""")
            timestamp = cursor.fetchone()[0]
            if timestamp:
                latest_timestamp = timestamp
            else:
                latest_timestamp = datetime.min
        else:
            latest_timestamp = datetime.min

        return latest_timestamp

    def get_sorted_articles(self, category, url, retry_count, recent=False):
        retry = 0
        offset = 0
        end_limit = False
        params = dict(self.params)
        params['category'] = category
        latest_timestamp = self.get_latest_timestamp(recent)

        while retry < retry_count and not end_limit:
            response = requests.get(url + urlencode(params), headers=self.headers)
            while response.status_code != 404:
                data = response.json()
                if data['pratilipiList']:
                    for article in data['pratilipiList']:
                        update_time = self.get_datetime(article['lastUpdatedDateMillis'])
                        if update_time > latest_timestamp:
                            if article['pratilipiId'] not in self.unique_ids:
                                self.unique_ids.add(article['pratilipiId'])
                                self.articles.append(article)
                        else:
                            end_limit = True
                            break

                    if end_limit:
                        break

                    offset += 20
                    params.update({
                        'offset': offset
                    })
                    next_url = url + urlencode(params)
                    response = self.session.get(next_url, headers=self.headers)
                else:
                    break

            if end_limit:
                break

            retry += 1
            time.sleep(3)

    def get_trending_articles(self):
        trending_url = 'https://hindi.pratilipi.com/api/list/v1.1?'
        params = {
            'listName': 'lovestories',
            'language': 'HINDI',
            'pratilipiResultCount': 100,
            'pratilipiCursor': '{%22type%22:%22reco%22,%22cursor%22:%220%22,%22meta%22:{}}'
        }
        url = trending_url + urlencode(params)
        response = requests.get(url, headers=self.headers)

        unique_ids = set()
        articles = []

        while len(response.json()['pratilipi']['pratilipiList']) > 0:
            if response.status_code != 200:
                continue

            data = response.json()['pratilipi']
            for article in data['pratilipiList']:
                if article['pratilipiId'] not in unique_ids:
                    unique_ids.add(article['pratilipiId'])
                    articles.append(article)

            cursor = data['pratilipiCursor']
            params.update({
                'pratilipiCursor': cursor
            })
            next_url = trending_url + urlencode(params)
            response = self.session.get(next_url, headers=self.headers)

        self.unique_ids = unique_ids
        self.articles = articles

    def get_popular_articles(self):
        high_rated_url = 'https://hindi.pratilipi.com/api/stats/v2.0/high_rated?'
        self.get_sorted_articles('lovestories', high_rated_url, 1)

    def get_recent_articles(self):
        recent_url = 'https://hindi.pratilipi.com/api/stats/v2.0/recent_published?'
        self.get_sorted_articles('lovestories', recent_url, 3, recent=True)

    @staticmethod
    def get_author_id_map(author_ids):
        formatted_string = ','.join(map(str, author_ids))
        cursor = execute_query("""SELECT pratilipi_id, id FROM pratilipi_authors WHERE pratilipi_id in ({ids})""".
                               format(ids=formatted_string))
        author_id_map = dict(cursor.fetchall())

        return author_id_map

    def process_authors(self, author_ids):
        author_url = 'https://hindi.pratilipi.com/api/authors/v1.0?authorId={author_id}'
        cursor = execute_query("""SELECT pratilipi_id FROM pratilipi_authors""")
        existing_ids = [tup[0] for tup in cursor]
        new_authors = set(author_ids) - set(existing_ids)

        insert_data = []
        for author_id in new_authors:
            url = author_url.format(author_id=author_id)
            response = requests.get(url, headers=self.headers)
            if response.status_code != 200:
                continue

            response = response.json()
            pratilipi_id = int(response['authorId'])
            author_name = response['fullName']
            author_name = author_name.replace("'", '"')
            follow_count = int(response['followCount'])
            read_count = int(response['totalReadCount'])
            language = response['language']
            gender = response['gender']
            page_url = response['pageUrl']
            site_registration_date = self.get_datetime(response['registrationDateMillis'])
            author_data = (author_name, follow_count, read_count, language, gender, pratilipi_id,
                           page_url, str(site_registration_date))
            insert_data.append(str(author_data))

        if len(insert_data):
            insertion_string = ','.join(insert_data)
            query = AUTHORS_INSERT_QUERY.format(data=insertion_string)
            execute_query(query)

    def process_articles(self, articles):
        author_ids = set()
        article_ids = set()
        for article in articles:
            author_id = article['author']['authorId']
            article_ids.add(article['pratilipiId'])
            if author_id not in author_ids:
                author_ids.add(author_id)

        self.process_authors(author_ids)
        author_map = self.get_author_id_map(author_ids)

        cursor = execute_query("""SELECT pratilipi_id FROM pratilipi_scripts""")
        existing_article_ids = [tup[0] for tup in cursor.fetchall()]

        insert_data = []
        for article in articles:
            pratilipi_id = article['pratilipiId']
            if pratilipi_id in existing_article_ids:
                continue

            author_id = article['author']['authorId']
            title = article['displayTitle']
            read_count = article['readCount']
            language = article['language']
            rating = article['averageRating']
            pratilipi_id = article['pratilipiId']
            page_url = article['pageUrl']
            site_updated_at = self.get_datetime(article['lastUpdatedDateMillis'])

            data = (title, read_count, language, rating, author_map[author_id], pratilipi_id, page_url,
                    str(site_updated_at))
            insert_data.append(str(data))

        if len(insert_data):
            insertion_string = ','.join(insert_data)
            query = SCRIPTS_INSERT_QUERY.format(data=insertion_string)
            execute_query(query)

    def __str__(self):
        return 'Pratilipi Spider'


if __name__ == "__main__":
    crawler = PratilipiSpider('HINDI')
    crawler.get_trending_articles()
    crawler.get_popular_articles()
    crawler.get_recent_articles()
    crawler.process_articles(crawler.articles)
