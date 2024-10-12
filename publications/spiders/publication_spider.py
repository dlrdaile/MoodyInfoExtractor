import scrapy
import json
import aiofiles
from publications.items import PublicationsItem
from pathlib import Path
from datetime import datetime
import time
from loguru import logger
import copy
import httpx


class PublicationSpider(scrapy.Spider):
    name = "publication_spider"
    start_urls = []
    json_data = {
        'urlPath': 'ratings-assessments-reports',
        'widgetVariant': 'Generic_ReportsDirectoryGeneric',
        'config': {
            'Generic_ReportsDirectoryGeneric': {
                'docTypes': [],
                'topics': [],
                'product': 'Generic',
                'variant': 'Reports Directory Generic',
                'excludeDocTypes': [
                    'Cash CDO Index Data',
                    'Cash CDO MAER Data',
                    'Cash CDO MDSR Data',
                    'Cash CDO MESR Data',
                    'Cash CDO MTAR Data',
                    'Cash CDO PO Data',
                    'Cash CDO Rating Changes Data',
                    'Historical Cash CDO Index Data',
                    'Historical Cash CDO PO Data',
                    'Historical Synth CDO Index Data',
                    'Historical Synth CDO PO Data',
                    'Synthetic CDO Index Data',
                    'Synthetic CDO PO Data',
                    'Performance Report - EMS',
                ],
                'docCount': 200,
                'languages': [
                    'English',
                ],
                'regionBoost': 0,
                'marketSegmentBoost': 0,
                'publicationDateCutoffMonths': 600,
                'publicationDateFrom': 946656000000,
                'publicationDateTo': 978278399999,
                'includeOrgDocs': False,
                'excludeOrgDocs': False,
                'useFacetDirect': False,
                'genericMarketSegmentCutoff': 0,
                'facets': [
                    'Research_News_rc',
                    'Issuer_Reports_rc',
                    'Sector_Reports_rc',
                    'Methodology_rc',
                    'Ratings_and_Assessments_Reports_rc',
                    'Regulatory_rc',
                    'Data_Report_rc',
                    'Market_Data_rc',
                    'Financial_Data_rc',
                    'Research_and_Data_Reports_rc',
                ],
                'facets_direct': [],
                'excludeSiblings': False,
                'disablePermHostCheck': False,
                'excludedFacets': [],
                'preserveDuplicateTitles': True,
                'aggregationSize': 1800,
                'entities': [],
                'series': [],
                'creditFoundations': [],
                'reportTypesFacets': [
                    'Ratings_News_rc',
                    'Ratings_List_rc',
                    'Rating_Action_rc',
                ],
                'sectorFacets': [
                    'Corporates',
                ],
                'countryFacets': [],
                'reportTitleSearch': 'downgrade',
            },
        },
        'page': 1,
    }

    cookies = {

    }

    headers = {
    }

    un_process_words = ['POSSIBLE DOWNGRADE', 'POSSIBLE FURTHER DOWNGRADE', 'POSSIBLE UPGRADE',
                        'POSSIBLE FURTHER UPGRADE',
                        'POSSIBLE STABLE', 'POSSIBLE FURTHER STABLE', 'POSSIBLE WATCH', 'POSSIBLE FURTHER WATCH',
                        'POSSIBLE REVIEW', 'POSSIBLE FURTHER REVIEW', 'POSSIBLE PLACEMENT',
                        'POSSIBLE FURTHER PLACEMENT',
                        'POSSIBLE WITHDRAW', 'POSSIBLE FURTHER WITHDRAW', 'POSSIBLE AFFIRM', 'POSSIBLE FURTHER AFFIRM',
                        'POSSIBLE OUTLOOK', 'POSSIBLE FURTHER OUTLOOK', 'POSSIBLE POSITIVE OUTLOOK',
                        'POSSIBLE FURTHER POSITIVE OUTLOOK', 'POSSIBLE NEGATIVE OUTLOOK',
                        'POSSIBLE FURTHER NEGATIVE OUTLOOK',
                        'POSSIBLE POSITIVE IMPLICATION', 'POSSIBLE FURTHER POSITIVE IMPLICATION',
                        'POSSIBLE NEGATIVE IMPLICATION', 'POSSIBLE FURTHER NEGATIVE IMPLICATION',
                        'POSSIBLE POSITIVE ACTION', 'POSSIBLE FURTHER POSITIVE ACTION', 'POSSIBLE NEGATIVE ACTION',
                        'POSSIBLE FURTHER NEGATIVE ACTION', 'POSSIBLE POSITIVE WATCH',
                        'POSSIBLE FURTHER POSITIVE WATCH',
                        'POSSIBLE NEGATIVE WATCH', 'POSSIBLE FURTHER NEGATIVE WATCH', 'POSSIBLE POSITIVE REVIEW',
                        'POSSIBLE FURTHER POSITIVE REVIEW', 'POSSIBLE NEGATIVE REVIEW',
                        'POSSIBLE FURTHER NEGATIVE REVIEW',
                        ]  # List of words to check for downgrade information

    def start_requests(self):
        # Initial POST request to start the pagination
        year_range = range(1995, 2025)
        # researchTitle = 'downgrade'
        search_list = ['downgrade', 'upgrade', 'outlook', 'lower']
        for researchTitle in search_list:
            for filter_year in year_range:
                save_path = Path('data') / str(filter_year)
                # 2000年1月1日的时间戳
                start_time = int(datetime(filter_year, 1, 1, 0, 0, 0).timestamp() * 1000)
                # 2000年12月31日 23:59:59的时间戳
                end_time = int(datetime(filter_year, 12, 31, 23, 59, 59).timestamp() * 1000)
                json_data = copy.deepcopy(self.json_data)
                json_data['config']['Generic_ReportsDirectoryGeneric']['publicationDateFrom'] = start_time
                json_data['config']['Generic_ReportsDirectoryGeneric']['publicationDateTo'] = end_time
                json_data['config']['Generic_ReportsDirectoryGeneric']['reportTitleSearch'] = researchTitle
                json_data['page'] = 1
                # if save_path.exists():
                #     logger.info(f'{filter_year}年数据已经存在，跳过')
                #     continue
                detail_path = save_path / 'detail'
                list_path = save_path / 'list'
                detail_path.mkdir(exist_ok=True, parents=True)
                list_path.mkdir(exist_ok=True, parents=True)
                yield scrapy.Request(
                    url='https://www.moodys.com/related-research/api/widget-search',
                    method='POST',
                    body=json.dumps(json_data),
                    cookies=self.cookies,
                    callback=self.parse,
                    headers=self.headers,
                    dont_filter=True,
                    meta={'detail_path': detail_path,
                          'json_data': json_data,
                          'researchTitle': researchTitle,
                          'list_path': list_path}
                )

    def parse(self, response):
        # Step 1: Parse the initial data response and generate data.json (simulates main.py functionality)
        data = response.json()

        if data is not None:
            # # Write the first page response to data1.json asynchronously
            # async with aiofiles.open(f'data{page}.json', 'w') as data_file:
            #     await data_file.write(json.dumps(data))

            # Calculate the total pages based on docCount
            doc_count = data['docCount']
            total_pages = doc_count // 200 + 1
            # print(total_pages)
            # Iterate through remaining pages and request data
            for i in range(1, total_pages + 1):
                json_data = response.meta['json_data']
                json_data['page'] = i
                save_path = response.meta['list_path'] / f'{response.meta["researchTitle"]}_data{i}.json'
                if save_path.exists():
                    try:
                        with open(save_path, 'r') as data_file:
                            data = json.load(data_file)
                            # logger.info(f'读取{save_path}成功')
                            yield from self.parse_page_data(data, response.meta['detail_path'])
                        continue
                    except Exception as e:
                        logger.error(f'读取{save_path}失败, {e}')
                yield scrapy.Request(
                    url='https://www.moodys.com/related-research/api/widget-search',
                    method='POST',
                    body=json.dumps(json_data),
                    cookies=self.cookies,
                    callback=self.parse_page,
                    headers=self.headers,
                    meta={'page': i, "list_path": response.meta['list_path'],
                          'researchTitle': response.meta['researchTitle'],
                          'save_path': save_path,
                          'detail_path': response.meta['detail_path']},
                    dont_filter=True
                )

    def parse_page(self, response):
        # Step 2: Parse additional pages and write to corresponding data files asynchronously
        # page = response.meta['page']
        data = response.json()
        if data is not None:
            # async with aiofiles.open(f'data{page}.json', 'w') as data_file:
            #     await data_file.write(json.dumps(data))
            # with open(response.meta['list_path'] / f'data{page}.json', 'w') as data_file:
            #     json.dump(data, data_file, indent=2)
            with open(response.meta['save_path'], 'w') as data_file:
                json.dump(data, data_file, indent=2)
            # async with aiofiles.open(save_path, 'w') as data_file:
            #     await data_file.write(json.dumps(data, indent=2))
            yield from self.parse_page_data(data, response.meta['detail_path'])

    def parse_page_data(self, data, detail_path):
        # Extract publication_id and proceed to get details
        for result in data['results']:
            publication_id = result['publication_id']
            detail_file_save_path = detail_path / f'{publication_id}.json'
            if detail_file_save_path.exists():
                # logger.info(f'{publication_id}已经存在，跳过')
                continue
            title = result.get('title', None)
            if title is None:
                # logger.info(f'{publication_id}没有标题，跳过')
                continue
            else:
                upper_title = title.upper()
                is_exist_un_process_word = False
                for word in self.un_process_words:
                    if word in upper_title:
                        is_exist_un_process_word = True
                        break
                if is_exist_un_process_word:
                    # logger.info(f'{publication_id}标题包含敏感词，跳过')
                    continue
            # if publication_id != '':
            #     continue
            yield scrapy.Request(
                url=f'https://www.moodys.com/research/api/research/{publication_id}',
                callback=self.parse_details,
                cookies=self.cookies,
                headers=self.headers,
                # dont_filter=True,
                meta={'publication_id': publication_id,
                      'detail_file_save_path': detail_file_save_path, }
            )

    async def parse_details(self, response):
        # Step 3: Parse the details and generate detail.json asynchronously (simulates get_detail.py functionality)
        publication_id = response.meta['publication_id']
        details = response.json()  # Adjust according to the actual response format
        if 'researchPayload' not in details:
            asp_path = details.get('s3Urls', None)
            if asp_path is not None:
                asp_path = asp_path.get('asp', None)
                if asp_path is not None:
                    async with httpx.AsyncClient() as client:
                        r = await client.get(f'https://www.moodys.com/research-document/{asp_path}', timeout=100)
                        if r.status_code == 200:
                            details['researchPayload'] = {
                                'entity_type': 'research_payload',
                                'html_content': r.text,
                                'publication_id': publication_id,
                                'parse_by_another_request': True,
                            }
                        else:
                            logger.warning(f'{publication_id}获取html失败')
        if 'researchPayload' not in details:
            logger.warning(f'{publication_id}没有找到researchPayload')
            return
        # Save details in detail.json asynchronously
        async with aiofiles.open(response.meta['detail_file_save_path'], 'w') as detail_file:
            await detail_file.write(json.dumps(details, indent=2))
        # Yield the details to Scrapy's pipeline or directly write them to the output file
        item = PublicationsItem()
        item['publication_id'] = publication_id
        item['detail_path'] = str(response.meta['detail_file_save_path'])
        # item['details'] = details
        yield item
