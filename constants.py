AUTHORS_INSERT_QUERY = """INSERT INTO pratilipi_authors (name, follow_count, read_count, language, gender,
                          pratilipi_id, page_url, site_registration_at) VALUES {data}"""

SCRIPTS_INSERT_QUERY = """INSERT INTO pratilipi_scripts (title, read_count, language, rating, author_id, pratilipi_id,
                          page_url, site_updated_at)VALUES {data}"""

SCRIPTS_DATA_QUERY = """SELECT * FROM pratilipi_scripts ORDER BY read_count DESC LIMIT {count}"""

POPULAR_URL = 'api/stats/v2.0/high_rated?'

RECENT_URL = 'api/stats/v2.0/recent_published?'

TRENDING_URL = 'api/list/v1.1?'

NAVIGATION_URL = 'api/navigation/list?language=HINDI'

AUTHOR_URL = 'api/authors/v1.0?authorId={author_id}'

DETAIL_PAGE_URL = 'api/pratilipis?slug={slug}'

AUTHOR_DETAILS_COLS = ['Author_Name', 'Follow_Count', 'Read_Count', 'Language', 'Gender',
                       'Pratilipi_Id', 'Page_Url', 'Registration_Date']

ARTICLE_DETAILS_COLS = ['Title', 'Read_Count', 'Read_Time', 'Tags', 'Author_Name', 'Language', 'Rating', 'Author_Id',
                        'Pratilipi_Id', 'Page_Url', 'Updated_At']
