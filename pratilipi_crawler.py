import time
from datetime import datetime
from urllib.parse import urlencode

import pandas as pd
import requests

from constants import AUTHORS_INSERT_QUERY, SCRIPTS_INSERT_QUERY, SCRIPTS_DATA_QUERY, POPULAR_URL, RECENT_URL, \
    TRENDING_URL, NAVIGATION_URL, AUTHOR_URL, AUTHOR_DETAILS_COLS, ARTICLE_DETAILS_COLS
from helpers import get_database_connection, execute_query, get_request_headers

TODAY = datetime.now()
CURRENT_TIME = str(TODAY.date()) + '_' + str(TODAY.hour)


class PratilipiCrawler:
    """
    Crawler to fetch new articles and authors on 'Pratilipi' website

    unique_ids: Category wise stores unique article data
    new_articles: New articles in all categories
    new_authors: New authors in all categories
    base_url: Main URL of website
    language: language
    headers: Request headers
    latest_timestamp: Last stored article timestamp
    existing_articles: Existing article ids in DB
    existing_authors: Existing author ids in DB

    """
    unique_ids = set()
    new_articles = set()
    new_authors = set()
    base_url = 'https://hindi.pratilipi.com/'

    def __init__(self, language):
        self.__db = get_database_connection()
        self.language = language
        self.headers = get_request_headers(self.base_url)
        self.latest_timestamp = self.get_latest_timestamp()
        self.existing_articles = self.get_existing_articles()
        self.existing_authors = self.get_existing_authors()

    @staticmethod
    def get_datetime(unix_timestamp):
        return datetime.fromtimestamp(unix_timestamp / 1000)

    def get_latest_timestamp(self):
        cursor = execute_query(
            """SELECT MAX(site_updated_at) FROM pratilipi_scripts""",
            self.__db
        )
        timestamp = cursor.fetchone()[0]

        return timestamp

    def get_categories(self):
        response = requests.get(self.base_url + NAVIGATION_URL,
                                headers=self.headers).json()
        categories = []
        for category_type in response['navigationList']:
            categories.extend(category_type['linkList'])

        return categories

    def get_article_params(
            self,
            category,
            trending=False
    ):
        if trending:
            article_params = {
                'language': self.language,
                'listName': category,
                'pratilipiResultCount': 100,
                'pratilipiCursor': ''
            }
        else:
            article_params = {
                'language': self.language,
                'category': category,
                'offset': 0,
                'fromSec': 300,
                'toSec': 1799
            }

        return article_params

    def get_existing_articles(self):
        cursor = execute_query(
            """SELECT pratilipi_id FROM pratilipi_scripts""",
            self.__db
        )
        article_ids = [tup[0] for tup in cursor.fetchall()]

        return article_ids

    def get_existing_authors(self):
        cursor = execute_query(
            """SELECT pratilipi_id FROM pratilipi_authors""",
            self.__db
        )
        author_ids = [tup[0] for tup in cursor.fetchall()]

        return author_ids

    def get_sorted_articles(
            self,
            category,
            page_url,
            recent=False
    ):
        """
        Processes data from recent, high_rated pages of a category
        :param category: Category
        :param page_url: Main page URL
        :param recent: Recent poge check (Bool)
        :return:
        """
        articles = []
        timestamp = datetime.min
        params = self.get_article_params(category)
        if recent and self.latest_timestamp:
            timestamp = self.latest_timestamp

        url = self.base_url + page_url
        response = requests.get(url + urlencode(params), headers=self.headers)
        while response.ok:
            data = response.json()
            if not data['pratilipiList']:
                break

            for article in data['pratilipiList']:
                update_time = self.get_datetime(article['lastUpdatedDateMillis'])
                # Get only new articles
                if update_time > timestamp:
                    if article['pratilipiId'] not in self.unique_ids:
                        self.unique_ids.add(article['pratilipiId'])
                        articles.append(article)
                else:
                    return articles

            # Offset pagination
            params.update({
                'offset': params['offset'] + 20
            })
            next_url = url + urlencode(params)
            response = requests.get(next_url, headers=self.headers)

            if response.status_code == 404:
                # Repeated response for recent page check after 3 seconds wait
                time.sleep(3)
                response = requests.get(url + urlencode(params), headers=self.headers)

        return articles

    def get_trending_articles(
            self,
            category,
            page_url
    ):
        """
        :returns: Category wise trending articles (List of JSON)
        """
        params = self.get_article_params(category, True)
        url = self.base_url + page_url
        first_url = url + urlencode(params)
        response = requests.get(first_url, headers=self.headers)
        articles = []

        while response.status_code != 404:
            data = response.json()['pratilipi']
            if not len(data) or not len(data['pratilipiList']):
                break

            for article in data['pratilipiList']:
                if article['pratilipiId'] not in self.unique_ids:
                    self.unique_ids.add(article['pratilipiId'])
                    articles.append(article)

            # Cursor pagination
            cursor = data['pratilipiCursor']
            params.update({
                'pratilipiCursor': cursor
            })
            next_url = url + urlencode(params)
            response = requests.get(next_url, headers=self.headers)

        return articles

    def process_authors(self, articles):
        """
        :returns Category wise new authors data (List of tuples)
        """
        author_ids = set()
        for article in articles:
            author_id = article['author']['authorId']
            if author_id not in author_ids.union(self.existing_authors):
                author_ids.add(author_id)

        author_data = []
        author_page_url = self.base_url + AUTHOR_URL
        for author_id in author_ids:
            url = author_page_url.format(author_id=author_id)
            response = requests.get(url, headers=self.headers)
            if response.status_code != 200:
                continue

            response = response.json()
            if response['displayName'] == '':
                continue
            pratilipi_id = int(response['authorId'])
            author_name = response['fullName']
            author_name = author_name.replace("'", '"')
            follow_count = int(response['followCount'])
            read_count = int(response['totalReadCount'])
            language = response['language']
            gender = response['gender']
            page_url = response['pageUrl']
            site_registration_date = self.get_datetime(
                response['registrationDateMillis']
            )

            data = (
                author_name,
                follow_count,
                read_count,
                language,
                gender,
                pratilipi_id,
                page_url,
                str(site_registration_date)
            )
            author_data.append(data)
            self.new_authors.add(data)

        return author_data

    def process_articles(self, articles):
        """
        :returns: Category wise new articles (List of tuples)
        """
        article_data = []
        for article in articles:
            pratilipi_id = article['pratilipiId']
            if pratilipi_id in self.existing_articles:
                continue

            author_id = article['author']['authorId']
            title = article['displayTitle']
            read_count = article['readCount']
            language = article['language']
            rating = article['averageRating']
            pratilipi_id = article['pratilipiId']
            page_url = article['pageUrl']
            site_updated_at = self.get_datetime(article['lastUpdatedDateMillis'])

            data = (
                title,
                read_count,
                language,
                rating,
                author_id,
                pratilipi_id,
                page_url,
                str(site_updated_at)
            )
            article_data.append(data)
            self.new_articles.add(data)

        return article_data

    def save_data_db(self, data_type, batch_size=1):
        data = []
        insertion_query = ''
        if data_type == 'authors':
            data = self.new_authors
            insertion_query = AUTHORS_INSERT_QUERY
        elif data_type == 'articles':
            data = self.new_articles
            insertion_query = SCRIPTS_INSERT_QUERY

        if len(data):
            insert_data = list(map(str, data))
            batch_pos = 0
            while batch_pos < len(insert_data):
                insertion_string = ','.join(insert_data[batch_pos: batch_pos + batch_size])
                query = insertion_query.format(data=insertion_string)
                execute_query(query, self.__db)
                batch_pos += batch_size

    @staticmethod
    def save_authors_csv(
            authors,
            category_name
    ):
        """
        Store authors to csv
        :param authors: Authors list
        :param category_name: Category
        """
        author_df = pd.DataFrame(authors, columns=AUTHOR_DETAILS_COLS)
        author_df.sort_values(
            'Follow_Count',
            ascending=False,
            inplace=True
        )

        filename = 'authors_{category}_{date}.csv'.format(
            category=category_name, date=CURRENT_TIME
        )
        author_df.to_csv(filename)

    def save_articles_csv(
            self,
            articles,
            count,
            category_name
    ):
        """
        Save articles to CSV
        :param articles: Articles List
        :param count: Count of Popular Articles
        :param category_name: Category
        """
        articles_df = pd.DataFrame(articles, columns=ARTICLE_DETAILS_COLS)
        articles_df['Updated_At'] = pd.to_datetime(articles_df['Updated_At'])
        time_sorted_articles = articles_df.sort_values('Updated_At', ascending=False)

        if self.latest_timestamp:
            latest_time = self.latest_timestamp
        else:
            latest_time = min(articles_df['Updated_At'])

        mask = time_sorted_articles['Updated_At'] >= latest_time
        recent_articles = time_sorted_articles[mask]

        if count > len(articles_df):
            cursor = execute_query(
                SCRIPTS_DATA_QUERY.format(count=count),
                self.__db
            )
            column_names = [i[0] for i in cursor.description]
            popular_articles = pd.DataFrame(cursor.fetchall(), columns=column_names)
            popular_articles = popular_articles.drop(['id', 'created_at'], 1)
        else:
            sorted_articles = articles_df.sort_values('Read_Count', ascending=False)
            popular_articles = sorted_articles.iloc[:count, :]

        recent_filename = 'recent_{category}_{date}.csv'.format(
            category=category_name, date=CURRENT_TIME
        )

        popular_filename = 'popular_{category}_{date}.csv'.format(
            category=category_name, date=CURRENT_TIME
        )

        recent_articles.to_csv(recent_filename)
        popular_articles.to_csv(popular_filename)

    def process_categories(self):
        """
        Process category wise data, get articles and authors -> store in CSV, database
        """
        categories = self.get_categories()
        for category in categories:
            self.unique_ids = set()
            category_name = category['categoryName']
            category_url = category['url'].strip('/')
            popular_articles = self.get_sorted_articles(category_url, POPULAR_URL)
            recent_articles = self.get_sorted_articles(category_url, RECENT_URL, recent=True)
            trending_articles = self.get_trending_articles(category_url, TRENDING_URL)
            all_articles = popular_articles + recent_articles + trending_articles

            authors_data = self.process_authors(all_articles)
            articles_data = self.process_articles(all_articles)

            self.save_authors_csv(authors_data, category_name)
            self.save_articles_csv(articles_data, len(articles_data), category_name)

        self.save_data_db('authors')
        self.save_data_db('articles')
        self.__db.close()


if __name__ == "__main__":
    crawler = PratilipiCrawler('HINDI')
    crawler.process_categories()
