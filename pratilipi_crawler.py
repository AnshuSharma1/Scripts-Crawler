import os
import time
from datetime import datetime
from urllib.parse import urlencode

import pandas as pd
import requests

from constants import AUTHORS_INSERT_QUERY, SCRIPTS_INSERT_QUERY, CATEGORY_INSERT_QUERY, \
    POPULAR_URL, RECENT_URL, TRENDING_URL, NAVIGATION_URL, AUTHOR_URL, DETAIL_PAGE_URL, AUTHOR_DETAILS_COLS, \
    ARTICLE_DETAILS_COLS
from helpers import get_database_connection, execute_query, get_request_headers

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
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
        category_url = category['url'].strip('/')
        params = self.get_article_params(category_url)
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
                    return self.process_articles(category, articles)

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

        return self.process_articles(category, articles)

    def get_trending_articles(
            self,
            category,
            page_url
    ):
        """
        :returns: Category wise trending articles (List of JSON)
        """
        category_url = category['url'].strip('/')
        params = self.get_article_params(category_url, True)
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

        return self.process_articles(category, articles)

    def process_authors(self, articles):
        """
        :returns Category wise new authors data (List of tuples)
        """
        author_ids = set()
        for article in articles:
            author_id = article[8]
            if author_id not in author_ids.union(self.existing_authors):
                author_ids.add(author_id)

        authors = set()
        author_page_url = self.base_url + AUTHOR_URL
        for author_id in author_ids:
            url = author_page_url.format(author_id=author_id)
            response = requests.get(url, headers=self.headers)
            if response.status_code != 200:
                continue

            response = response.json()
            if response['displayName'] == '':
                continue

            try:
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
            except KeyError:
                continue

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
            authors.add(data)

        return authors

    def get_article_tags(self, article):
        tags = []
        url = self.base_url + DETAIL_PAGE_URL
        if 'tags' in article.keys():
            for tag in article['tags']:
                tags.append(tag['nameEn'])
        else:
            slug = article['slug'].split('/')[-1]
            detail_page = requests.get(url.format(slug=slug), headers=self.headers)
            if detail_page.status_code == 200:
                response = detail_page.json()
                if 'tags' in response.keys():
                    tags = [tag['nameEn'] for tag in response['tags']]

        tags = list(filter(None.__ne__, tags))
        return tags

    def get_reading_time(self, article):
        read_time = 0
        key = 'readingTime'
        url = self.base_url + DETAIL_PAGE_URL
        if key in article.keys():
            read_time = article[key]
        else:
            slug = article['slug'].split('/')[-1]
            detail_page = requests.get(url.format(slug=slug), headers=self.headers)
            if detail_page.status_code == 200:
                response = detail_page.json()
                if key in response.keys():
                    read_time = response[key]

        return read_time

    def process_articles(self, category, articles):
        """
        :returns: Category wise new articles (List of tuples)
        """
        category_name = category['categoryName']
        article_data = []
        for article in articles:
            pratilipi_id = article['pratilipiId']
            if pratilipi_id in self.existing_articles:
                continue

            try:
                author_details = article['author']
                author_id = author_details['authorId']
                title = article['displayTitle'].replace("'", '"')
                read_time = self.get_reading_time(article)
                read_count = article['readCount']
                language = article['language']
                rating = article['averageRating']
                pratilipi_id = article['pratilipiId']
                page_url = article['pageUrl']
                site_updated_at = self.get_datetime(article['lastUpdatedDateMillis'])
                tags = self.get_article_tags(article)
                if 'displayName' in author_details:
                    author_name = author_details['displayName'].replace("'", '"')
                else:
                    author_name = ''
            except KeyError:
                continue

            data = (
                title,
                read_count,
                read_time,
                category_name,
                ','.join(tags),
                author_name,
                language,
                rating,
                author_id,
                pratilipi_id,
                page_url,
                str(site_updated_at)
            )
            article_data.append(data)

        return article_data

    def save_data_db(self, data, insertion_query, batch_size=1):
        if not len(data):
            return

        insert_data = list(map(str, data))
        batch_pos = 0
        while batch_pos < len(insert_data):
            insertion_string = ','.join(insert_data[batch_pos: batch_pos + batch_size])
            query = insertion_query.format(data=insertion_string)
            execute_query(query, self.__db)
            batch_pos += batch_size

    @staticmethod
    def save_authors_csv(
            authors
    ):
        """
        Store authors to csv
        :param authors: Authors list
        """
        if not len(authors):
            return

        author_df = pd.DataFrame(authors, columns=AUTHOR_DETAILS_COLS)
        author_df.sort_values(
            'Follow_Count',
            ascending=False,
            inplace=True,
            ignore_index=True
        )

        filename = 'authors_{date}.csv'.format(date=CURRENT_TIME)
        author_df.to_csv(filename)

    def save_articles_csv(
            self,
            articles,
            count
    ):
        """
        Save articles to CSV
        :param articles: Articles List
        :param count: Count of Popular Articles
        """
        if not len(articles):
            return []

        articles_df = pd.DataFrame(articles, columns=ARTICLE_DETAILS_COLS).drop_duplicates()
        articles_df['Updated_At'] = pd.to_datetime(articles_df['Updated_At'])
        if self.latest_timestamp:
            latest_time = self.latest_timestamp
        else:
            latest_time = min(articles_df['Updated_At'])

        time_sorted_articles = articles_df.sort_values(
            'Updated_At',
            ascending=False,
            ignore_index=True
        )
        mask = time_sorted_articles['Updated_At'] >= latest_time
        recent_articles = time_sorted_articles[mask]

        read_sorted_articles = articles_df.sort_values(
            'Read_Count',
            ascending=False,
            ignore_index=True
        )
        popular_articles = read_sorted_articles.iloc[:count, :]

        recent_filename = 'recent_{date}.csv'.format(date=CURRENT_TIME)
        popular_filename = 'popular_{date}.csv'.format(date=CURRENT_TIME)
        recent_articles.to_csv(recent_filename)
        popular_articles.to_csv(popular_filename)

    @staticmethod
    def get_articles_df(articles):
        frame = pd.DataFrame(articles, columns=ARTICLE_DETAILS_COLS)
        frame.drop_duplicates(inplace=True)
        frame['Updated_At'] = pd.to_datetime(frame['Updated_At'])

        return frame

    def get_genre_data(self, articles):
        if not len(articles):
            return []

        genre_query = """
        SELECT pratilipi_id, category FROM pratilipi_categories WHERE pratilipi_id in ({ids})
        """
        genre = articles[['Pratilipi_Id', 'Genre']].drop_duplicates()
        ids = ','.join(map(str, genre['Pratilipi_Id'].unique()))
        cursor = execute_query(genre_query.format(ids=ids), self.__db)
        existing_data = pd.DataFrame(cursor.fetchall(), columns=genre.columns)
        id_mask = (genre['Pratilipi_Id'].isin(existing_data['Pratilipi_Id']))
        genre_mask = (genre['Genre'].isin(existing_data['Genre']))
        new_data = genre[~(id_mask & genre_mask)]

        return list(map(tuple, new_data.values))

    def process_categories(self):
        """
        Process category wise data, get articles and authors -> store in CSV, database
        """
        categories = self.get_categories()
        all_data = []
        for category in categories:
            self.unique_ids = set()
            recent_articles = self.get_sorted_articles(category, RECENT_URL, recent=True)
            popular_articles = self.get_sorted_articles(category, POPULAR_URL)
            trending_articles = self.get_trending_articles(category, TRENDING_URL)
            all_articles = recent_articles + popular_articles + trending_articles
            all_data.extend(all_articles)

        authors_data = self.process_authors(all_data)
        articles_df = self.get_articles_df(all_data)
        article_db_data = articles_df.drop(['Genre', 'Author_Name'], 1)
        article_db_data = article_db_data.drop_duplicates(subset=['Pratilipi_Id']).values
        article_db_data = list(map(tuple, article_db_data))
        genre_data = self.get_genre_data(articles_df)

        self.save_authors_csv(authors_data)
        self.save_articles_csv(articles_df, len(articles_df))
        self.save_data_db(authors_data, AUTHORS_INSERT_QUERY)
        self.save_data_db(article_db_data, SCRIPTS_INSERT_QUERY)
        self.save_data_db(genre_data, CATEGORY_INSERT_QUERY)
        self.__db.close()


if __name__ == "__main__":
    crawler = PratilipiCrawler('HINDI')
    crawler.process_categories()
