import MySQLdb
from configparser import ConfigParser


def read_from_config(section):
    config_parser = ConfigParser()
    config_file_path = r'config.ini'
    config_parser.read(config_file_path)

    return dict(config_parser.items(section))


def execute_query(query):
    cred = read_from_config('database-read')
    database = MySQLdb.connect(host=cred['host'],
                               user=cred['user'],
                               passwd=cred['password'],
                               db=cred['db'],
                               port=cred['port'])
    cursor = database.cursor()
    cursor.execute(query)
    database.commit()
    query_result_list = [row for row in cursor.fetchall()]

    return query_result_list


