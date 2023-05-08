import aiohttp
import asyncio
import datetime
from fp.fp import FreeProxy
from fp.errors import FreeProxyException
from bs4 import BeautifulSoup
import time
import csv
import sys


root = 'https://www.qrz.com'
birthday_url = f'{root}/callbook/birthday/'
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/41.0.2228.0 Safari/537.36'
}


def date_generator():
    start_date = datetime.date(datetime.date.today().year, 1, 1)
    end_date = datetime.date(datetime.date.today().year, 12, 31)
    current_date = start_date

    while current_date <= end_date:
        yield f'{current_date.month:02}-{current_date.day:02}'
        current_date += datetime.timedelta(days=1)


def find_proxy():
    try:
        proxy = FreeProxy(country_id=['FI, CA, JP, NL']).get()
    except FreeProxyException:
        sys.exit('[ERROR] There is no available proxy found')

    print(f'PROXY found! {proxy}')
    return proxy


async def get_page(session, proxy, url):
    async with session.get(url, proxy=proxy) as response:
        return url, await response.text()


async def get_all_pages(session, proxy, urls):
    tasks = []
    for url in urls:
        tasks.append(asyncio.create_task(get_page(session, proxy, url)))
    return await asyncio.gather(*tasks)


def parse_birthdate_pages(pages):
    birthdays = {}
    for url, html in pages:
        soup = BeautifulSoup(html, 'html.parser')
        links = soup.select('table a')
        date = url.split('/')[-1]
        birthdays[date] = [root + a['href'] for a in links]
    return birthdays


def parse_amateur_pages(pages):
    amateurs = []
    for _, html in pages:
        soup = BeautifulSoup(html, 'html.parser')

        amateur_info_html = [
            soup.select('div.main_content h1'),  # call_sign
            soup.select('#infoBlock b'),  # name_original
            soup.select('#infoBlock div[style="color:gray;"] b'),  # name_eng
        ]
        amateurs.append(
            [' '.join(x[0].text.split()) if x else 'No info' for x in amateur_info_html])
    return amateurs


async def main(proxy=None, filepath='./amateurs'):
    urls = [birthday_url + date_str for date_str in date_generator()]
    trust = True if proxy else False

    async with aiohttp.ClientSession(headers=headers, trust_env=trust) as session:
        birthdate_pages = await get_all_pages(session, proxy, urls)
        birthdays = parse_birthdate_pages(birthdate_pages)
        for date, links in birthdays.items():
            amateur_pages = await get_all_pages(session, proxy, links)
            amateurs = [amateur_info + [date] for amateur_info in parse_amateur_pages(amateur_pages)]

            with open(filepath, mode='w', newline='', encoding="utf-8") as csvfile:
                fieldnames = ['call_sign', 'name_original', 'name_english', 'birthdate']
                writer = csv.writer(csvfile, delimiter=",", quotechar="'")
                writer.writerow(fieldnames)
                writer.writerows(amateurs)


if __name__ == '__main__':
    proxy = find_proxy()

    start_time = time.perf_counter()
    asyncio.run(main(proxy))
    print(f'Execution time: {time.perf_counter() - start_time} seconds')
