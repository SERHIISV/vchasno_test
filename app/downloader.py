import os
import yaml
import asyncio
import asyncpg
import async_timeout

import xml.etree.ElementTree as ET

from aiohttp import ClientSession
from datetime import datetime


### Loading configs
project_dir = os.path.dirname(os.path.realpath(__file__))

### Get configs list
configs = yaml.load(open(project_dir + '/config.yml').read())

### Feeds urls
urls = yaml.load(open(project_dir + '/feeds.yml').read())

async def download(url, session, pool):
    """ 
    This function downloading feed then parse xml then save data to database

    """
    with async_timeout.timeout(10):
        async with session.get(url) as response:
            while True:
                ### Read response data
                data = await response.content.read()
                if not data:
                    break
                root = ET.fromstring(data)
                items = root.findall('channel/item')
                for item in items:
                    title = item.find('title').text
                    pub_date = item.find('pubDate').text
                    try:
                        pub_date_format = "'" + datetime.strptime(pub_date, '%a, %d %b %Y %H:%M:%S GMT') + "'"
                    except:
                        pub_date_format = 'NULL'
                    url = item.find('link').text

                    ### Prepare sql query string for saving data
                    query = configs['INSERT_TO_DB'].format(
                        configs['Table'],
                        str(title.replace("'", "\'") if title else title),
                        pub_date_format,
                        str(url.replace("'", "\'") if url else url),
                    )

                    ## Save data to database
                    try:
                        async with pool.acquire() as conn:
                            await conn.execute(query)
                    except:
                        print('Error, cannot save data to database')
                        pass

async def main():
    """ 
    This function create pool connection and create client session for downloading feeds

    """
    ### Create pool connection
    async with asyncpg.create_pool(user=configs['User'],
                                   password=configs['Password'],
                                   database=configs['Database'],
                                   host=configs['Host']) as pool:

        print('Start downloading data...')

        ### Create session and tasks
        async with ClientSession() as session:
            tasks = [download(url, session, pool) for url in urls['Feeds']]
            await asyncio.gather(*tasks)

    print('Finished')
    print('Next run after {period}s'.format(period=configs['Period']))

    ### Waiting next run
    await asyncio.sleep(configs['Period'])
    await main()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        loop.create_task(main())
        loop.run_forever()
    finally:
        print('Closing even loop')
        loop.close()
