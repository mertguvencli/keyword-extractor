import re
import requests
import ssl
import uuid
from bs4 import BeautifulSoup
import time
from fake_useragent import UserAgent
from utils import Db
import logging

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

Db().init_objects()
ssl._create_default_https_context = ssl._create_unverified_context
user_agent = UserAgent()


def jobs_list_request(keyword, location, start=0):
    params = {
        "keywords": keyword,
        "location": location,
        "geoId": "",
        "trk": "public_jobs_jobs-search-bar_search-submit",
        "start": start,
    }
    url = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
    headers = {'User-Agent': user_agent.random}
    return requests.get(url=url, headers=headers, params=params)


def job_detail_request(row_id, job_id) -> None:
    url = f'https://www.linkedin.com/jobs/view/{job_id}'
    try:
        start_time = time.time()    
        headers = {'User-Agent': user_agent.random}
        response = requests.get(url=url, headers=headers, timeout=2)
        elapsed = time.time()-start_time
        logging.info(f'job_id: {job_id} status_code: {response.status_code} elapsed: {elapsed:.2f}')

        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            company = soup.find(attrs={"class": "topcard__flavor--black-link"})
            if not company:
                company = soup.find(attrs={"class": "topcard__flavor"})
            company = company.text.strip()
            location = soup.find(attrs={"class": "topcard__flavor--bullet"}).text.strip()
            description = soup.find(attrs={"class": "show-more-less-html__markup"})
            description = description.getText(separator='\n', strip=True)

            Db().update_job(company, location, description, row_id)

        if response.status_code in [400, 404]:
            # The post has probably been deleted or deactivated.
            logging.warn('Deleting job_id: {job_id}')
            Db().delete_job(row_id)
    except:
        ...


def parse_job_list(keyword, country, page, task_id) -> None:
    soup = BeautifulSoup(page, 'html.parser')
    data = []
    for container in soup.find_all(attrs={"class": "job-search-card"}):
        job_id = int(re.findall(r'\d+', container['data-entity-urn'])[0])
        title = container.find(attrs={"class": "base-search-card__title"}).text.strip()
        salary = container.find(attrs={"class": "job-search-card__salary-info"})
        salary = salary.text.strip() if salary else None
        company = container.find(attrs={"class": "base-search-card__subtitle"}).text.strip()

        data.append((task_id, keyword, country, job_id, company, title, salary))  # noqa
        logging.info(f'job_id: {job_id} title: {title}')

    Db().add_jobs(data)


def process_jobs(keyword, location, task_id):
    start = 0
    while True:
        prev = start

        response = jobs_list_request(keyword=keyword, location=location, start=start)

        if response.status_code != 200:
            if start >= 975:
                # Waiting for security check
                # Linkedin redirects to https://www.linkedin.com/authwall
                break

            logging.warning('Waiting ...')
            time.sleep(1.2)
            response = jobs_list_request(keyword=keyword, location=location, start=start)

        if response.status_code == 200:
            parse_job_list(keyword, location, page=str(response.content, "utf-8"), task_id=task_id)
            
            start += 25

        logging.info(f'location: {location} status_code: {response.status_code} task_id: {task_id} page: {prev}-{start}')


if __name__ == '__main__':
    keywords = ["Data Scientist"]
    locations = [
        "United States", "India", "Germany", "United Kingdom", "Canada",
        "France", "Brazil", "Poland", "Netherlands", "Italy"
    ]

    for keyword in keywords:
        for location in locations:
            task_id = uuid.uuid4().hex

            process_jobs(keyword, location, task_id)

            for item in Db().get_not_ready_jobs():
                job_detail_request(item[0], item[1])
