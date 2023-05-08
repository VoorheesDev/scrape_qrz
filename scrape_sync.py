import datetime
import requests
from fp.fp import FreeProxy
from fp.errors import FreeProxyException
from bs4 import BeautifulSoup
import time
import csv
import sys
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


root = 'https://www.qrz.com'
birthday_url = f'{root}/callbook/birthday/'


def date_generator():
    start_date = datetime.date(datetime.date.today().year, 1, 1)
    end_date = datetime.date(datetime.date.today().year, 12, 31)
    current_date = start_date

    while current_date <= end_date:
        yield f'{current_date.month:02}-{current_date.day:02}'
        current_date += datetime.timedelta(days=1)


def create_session(connect_retries_number=10, between_attempts_factor=0.2):
    session = requests.Session()
    retry = Retry(connect=connect_retries_number, backoff_factor=between_attempts_factor)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def bind_proxy(session):
    try:
        proxies = {
            'http': FreeProxy(country_id=['FI, JP, CA, NL']).get(),
            'https': FreeProxy(country_id=['FI, JP, CA, NL'], https=True).get(),
        }
    except FreeProxyException:
        sys.exit('[ERROR] There is no available proxy found')

    print('PROXY found!')
    session.proxies.update(proxies)


def make_request(session, url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/41.0.2228.0 Safari/537.36'
    }
    try:
        return session.get(url, headers=headers)
    except requests.exceptions.ProxyError:
        sys.exit('[ERROR] All attempts to connect to proxy failed')


def main(session, filepath='./amateurs'):
    with open(filepath, mode='w', newline='', encoding="utf-8") as csvfile:
        fieldnames = ['call_sign', 'name_original', 'name_english', 'birthdate']
        writer = csv.writer(csvfile, delimiter=",", quotechar="'")
        writer.writerow(fieldnames)

        # scrape and parse table of amateurs for every day in a year
        for date in date_generator():
            response = make_request(session, birthday_url + date)
            soup = BeautifulSoup(response.text, 'html.parser')
            links = [root + a['href'] for a in soup.select('table a')]

            # scrape and save info for each individual amateur
            for url in links:
                response = make_request(session, url)
                soup = BeautifulSoup(response.text, 'html.parser')

                amateur_info_html = [
                    soup.select('div.main_content h1'),  # call_sign
                    soup.select('#infoBlock b'),  # name_original
                    soup.select('#infoBlock div[style="color:gray;"] b'),  # name_eng
                ]

                amateur_info = [' '.join(x[0].text.split()) if x else 'No info' for x in amateur_info_html]
                writer.writerow(amateur_info + [date])


if __name__ == '__main__':
    session = create_session()
    bind_proxy(session)

    start_time = time.perf_counter()
    main(session)
    print(f'Execution time: {time.perf_counter() - start_time} seconds')

