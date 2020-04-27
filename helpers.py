from configparser import ConfigParser

import MySQLdb
import requests
from MySQLdb.connections import OperationalError
from fake_useragent import UserAgent


def read_from_config(section):
    config_parser = ConfigParser()
    config_file_path = 'config.ini'
    config_parser.read(config_file_path)

    return dict(config_parser.items(section))


def execute_query(query, retry=0):
    cred = read_from_config('database-read')
    try:
        database = MySQLdb.connect(host=cred['host'],
                                   user=cred['user'],
                                   passwd=cred['password'],
                                   db=cred['db'],
                                   port=int(cred['port']))
        cursor = database.cursor()
        cursor.execute(query)
        database.commit()
        return cursor
    except OperationalError as err:
        if retry < 3:
            execute_query(query, retry + 1)
        else:
            raise err


def get_request_headers(base_url):
    user_agent = UserAgent()
    response = requests.get(base_url)
    words = list(response.headers['Set-Cookie'].split())
    access_token = ''
    for word in words:
        if 'access_token' in word:
            access_token = word.strip(';')
            break

    headers = {
        'Cookie': access_token,
        'User-Agent': str(user_agent.random)
    }

    return headers
