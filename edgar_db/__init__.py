import concurrent.futures
import json
import os
from datetime import datetime
from zipfile import ZipFile

import requests

try:
    from tqdm import tqdm
except ImportError:
    class Mock:
        def __init__(self, *args, **kwargs):
            pass

        def __getattr__(self, *args, **kwargs):
            return Mock

    tqdm = Mock

__all__ = ['DB', 'CompanyFacts', 'Field']

COMPANY_FACTS_URL = 'https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip'
SUBMISSION_URL = 'https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip'

HEADERS = {"User-Agent": "Mozilla/5.0"}


def build():
    edgar_db = DB(path=os.getcwd())
    edgar_db.build()


class DB:
    def __init__(self, path: str = None):
        if not os.path.isdir(path):
            raise ValueError('EDGAR needs a valid folder to store data')

        self.path = path
        self.company_facts_zip = os.path.join(self.path, 'company_facts.zip')
        self.company_facts_dir = os.path.join(self.path, 'company_facts')
        self.submissions_zip = os.path.join(self.path, 'submissions.zip')
        self.submissions_dir = os.path.join(self.path, 'submissions')
        self.summary_path = os.path.join(self.path, 'summary.json')
        self.ticker_index_path = os.path.join(self.path, 'ticker.json')
        self.company_name_index_path = os.path.join(self.path, 'company.json')

        for d in [self.company_facts_dir, self.submissions_dir]:
            os.makedirs(d, exist_ok=True)

        try:
            self.ticker_index = json.load(open(self.ticker_index_path))
        except:
            self.ticker_index = {}

        try:
            self.company_name_index = json.load(open(self.company_name_index_path))
        except:
            self.company_name_index = {}

    def build(self):
        # os.rmdir(self.path)
        # for d in [self.path, self.company_facts_dir, self.submissions_dir]:
        #     os.makedirs(d, exist_ok=True)

        now = datetime.now()

        process_zip(COMPANY_FACTS_URL, self.company_facts_zip, self.company_facts_dir)
        process_zip(SUBMISSION_URL, self.submissions_zip, self.submissions_dir)

        for file_name in tqdm(os.listdir(self.company_facts_dir), desc="Building Indices", leave=True):
            try:
                with open(os.path.join(self.company_facts_dir, file_name)) as file:
                    data = json.load(file)
                    cik = data['cik']
                    cik = '0' * (10 - len(cik)) + cik

                    self.ticker_index.update({ticker: cik for ticker in data['tickers']})
                    self.company_name_index[data['entityName']] = cik
            except:
                pass

        with open(self.ticker_index_path, 'w+') as ticker_index_file:
            ticker_index_file.write(json.dumps(self.ticker_index, indent=4))

        with open(self.company_name_index_path, 'w+') as company_index_file:
            company_index_file.write(json.dumps(self.company_name_index, indent=4))

        with open(self.summary_path, 'w+') as summary_file:
            summary = {
                'start': now.isoformat(),
                'end': datetime.now().isoformat(),
                'total_size': sum(d.stat().st_size for d in os.scandir(self.path) if d.is_file()),
                'company_facts': {
                    # 'size': sum(d.stat().st_size for d in os.scandir(_company_facts) if d.is_file()),
                    'files': len(os.listdir(self.company_facts_dir)),
                    'zip_size': os.path.getsize(self.company_facts_zip),
                },
                'submissions': {
                    # 'size': sum(d.stat().st_size for d in tqdm(os.scandir(_submissions)) if d.is_file()),
                    'files': len(os.listdir(self.submissions_dir)),
                    'zip_size': os.path.getsize(self.submissions_zip),
                }
            }
            summary_file.write(json.dumps(summary, indent=4))

    def get(self, unique_id):
        cik = self.ticker_index.get(unique_id) or self.company_name_index.get(unique_id) or unique_id

        facts_path = os.path.join(self.company_facts_dir, f'CIK{cik}.json')
        if not cik or not os.path.isfile(facts_path):
            raise ValueError('No valid cik, ticker, or company name supplied')

        return CompanyFacts(json.load(open(facts_path)))


class CompanyFacts:
    def __init__(self, company_facts: dict):
        self.__raw_facts = company_facts

    def get(self, field_name: str):
        for form, field_data in self.__raw_facts['facts'].items():
            if field_name in field_data:
                return Field(form, field_name, field_data[field_name])


class Field:
    def __init__(self, form, name, data):
        self.form = form
        self.name = name
        self.data = data

    @property
    def label(self):
        return self.data['label']

    @property
    def description(self):
        return self.data['description']

    def __iter__(self):
        return ((item['fy'], item['fp'], item['val']) for item in self.data['units']['USD'])


def process_zip(url, zip_path, folder_path):
    with requests.get(url, headers=HEADERS, stream=True) as response:
        response.raise_for_status()
        with open(zip_path, 'wb+') as file:
            download_bar = tqdm(total=int(response.headers['Content-Length']), unit='1b', unit_scale=True, desc=url, leave=True)
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
                download_bar.update(len(chunk))

    if len(os.listdir(folder_path)) <= 0:
        with ZipFile(zip_path, 'r') as zip_file:
            zip_file.extractall(folder_path)
