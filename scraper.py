from dotenv import load_dotenv
import os
import requests
from bs4 import BeautifulSoup, Tag
import pyodbc
from urllib.parse import urljoin
import logging
import time
import random
import re

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_random_delay() -> float:
    return random.uniform(1, 2)

def connect_to_database(server: str, database: str, driver: str) -> pyodbc.Connection:
    ''' Establishes a connection to the SQL Server database. '''
    try:
        connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes;'
        conn = pyodbc.connect(connection_string)
        logging.info('Database connection established')
        return conn
    except Exception as e:
        logging.critical(f'Failed to connect to the database: {e}', exc_info=True)
        raise

def fetch_category_links(base_url: str) -> list[str]:
    ''' Fetch category links from the base URL. '''
    try:
        response = requests.get(base_url)
        response.raise_for_status()
        time.sleep(get_random_delay())
        soup = BeautifulSoup(response.text, 'html.parser')
        links = soup.find('ul', id='/university-course-descriptions/')
        if links:
            category_links = [
                urljoin(base_url, link.find('a').get('href'))
                for link in links.find_all('li') # type: ignore
                if link.find('a')
            ]
            logging.info(f'Fetched {len(category_links)} category links.')
            return category_links
        logging.warning('No category links found')
        return []
    except requests.exceptions.RequestException as e:
        logging.error(f'Error fetching category links: {e}')
        return []

def fetch_subject_links(category: str) -> list[str]:
    ''' Fetches the links to all subjects from a category. '''
    
    try:
        response = requests.get(category)
        response.raise_for_status()
        time.sleep(get_random_delay())
        soup = BeautifulSoup(response.text, 'html.parser')
        ul_elements = soup.find('div', class_='az_sitemap').find_all('ul')  # type: ignore
        subject_links = [
            urljoin(category, li.find('a').get('href'))
            for ul in ul_elements for li in ul.find_all('li')
            if li.find('a') and not li.find('a').get('href').startswith('#')
        ]
        logging.info(f'Fetched {len(subject_links)} subject links from {category}.')
        return subject_links
    except requests.exceptions.HTTPError as e:
        logging.error(f'HTTP error fetching subject links from {category}: {e}')
        return []
    except Exception as e:
        logging.error(f'Error fetching subject links from {category}: {e}')
        return []

def fetch_courses(subject: str) -> list[Tag]:
    ''' Fetches all the courses on a subject page. '''
    response = requests.get(subject)
    response.raise_for_status()
    time.sleep(get_random_delay())
    soup = BeautifulSoup(response.text, 'html.parser')
    return soup.find('div', class_='sc_sccoursedescs').find_all('div', class_='courseblock') # type: ignore

def extract_course_info(course: Tag) -> tuple[str, str, str, str, str]:
    ''' Extracts the course code, name, number of credits, description, and attributes from a course. '''
    details_div = course.find('div', class_='courseblocktitle_bubble')
    code = ' '.join(span.get_text() for span in details_div.find('div', class_='course_code').find_all('span')) # type: ignore
    name = details_div.find('div', class_='course_codetitle').get_text(strip=True) # type: ignore
    credit_text = details_div.find('div', class_='course_credits').get_text(strip=True) # type: ignore
    
    credit_nums = []
    for char in credit_text:
        if not char.isnumeric():
            break
        credit_nums.append(char)
            
    credit_hours = ''.join(credit_nums)
    
    try:
        description = course.find('div', class_='courseblockdesc').find('p').get_text(strip=True) # type: ignore
    except:
        description = 'N/A'
        
    try:
        attributes_paragraphs = course.find('div', class_='courseblockextra').findAll('p') # type: ignore
        attrbiutes_list = []
        for attribute in attributes_paragraphs:
            text = attribute.get_text()
            if 'Objective' not in text:
                clean_text = re.sub(r'[\s\xa0]+', ' ', text)
                attrbiutes_list.append(clean_text.strip())
        attributes = ', '.join(attrbiutes_list) + ' '
    except:
        attributes = 'N/A'

    return code, name, credit_hours, description, attributes

def insert_courses(cursor: pyodbc.Cursor, courses : list[tuple[str, str, str, str, str]]) -> None:
    ''' Inserts a batch of courses into the database. '''
    cursor.executemany(
        'INSERT INTO Classes (Code, Name, Credits, Description, Attributes) VALUES (?, ?, ?, ?, ?)',
        courses
    )

def main() -> None:
    ''' Orchestrate the scraping and database insertion process. '''
    load_dotenv()
    
    SERVER = os.getenv('SERVER')
    DATABASE = os.getenv('DATABASE')
    DRIVER = os.getenv('DRIVER')
    BASE_URL = 'https://bulletins.psu.edu/university-course-descriptions/'
    BATCH_SIZE = 100

    with connect_to_database(SERVER, DATABASE, DRIVER) as conn: # type: ignore
        with conn.cursor() as c: # type: ignore
            category_links = fetch_category_links(BASE_URL)
            counter = 0
            batch = []
            existing_codes = set()
            c.execute('SELECT Code FROM Classes')
            for row in c.fetchall():
                existing_codes.add(row[0])
            
            for category in category_links:
                try:
                    subjects = fetch_subject_links(category)
                    for subject in subjects:
                        try:
                            courses = fetch_courses(subject)
                            if not courses:
                                logging.warning(f'No courses found for subject: {subject}')
                                continue
                            for course in courses:
                                code, name, credits, description, attributes = extract_course_info(course)
                                
                                if code not in existing_codes:
                                    batch.append((code, name, credits, description, attributes))
                                    counter += 1
                                    existing_codes.add(code)
                                    logging.info(f'Prepared entry for {code}: {name}')
                                else:
                                    logging.info(f'Course already in database: {code} {name}')

                                if len(batch) >= BATCH_SIZE:
                                    insert_courses(c, batch)
                                    conn.commit()
                                    logging.info(f'Inserted {len(batch)} courses into the database. Last course added: {batch[-1][0]}')
                                    batch.clear()
                        
                        except Exception as e:
                            logging.error(f'Error processing course {subject}: {e}')
                            continue

                except requests.exceptions.RequestException as e:
                    logging.error(f'Request error while processing {category}: {e}')
                except Exception as e:
                    logging.error(f'Error processing row from {category}: {e}')

            if batch:
                insert_courses(c, batch)
                conn.commit()
                logging.info(f'Inserted remaining {len(batch)} courses into the database. Last course added: {batch[-1][0]}')
            
            logging.info(f'Total courses added: {counter}')
            
if __name__ == '__main__':
    main()
