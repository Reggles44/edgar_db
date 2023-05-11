import os
import json
import requests
import concurrent.futures
from datetime import datetime
from zipfile import ZipFile


__all__ = ['EDGAR', 'CompanyFacts', 'Field', 'HEADERS']

COMPANY_FACTS_URL = 'https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip'
SUBMISSION_URL = 'https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip'

HEADERS = {"User-Agent": "Mozilla/5.0"}


class EDGAR:
    def __init__(self, path: str):
        self.path = path
        self._company_facts = os.path.join(self.path, 'company_facts')
        self._submissions = os.path.join(self.path, 'submissions')
        self._summaries = os.path.join(self.path, 'summaries')

        for d in [self.path, self._company_facts, self._submissions, self._summaries]:
            os.makedirs(d, exist_ok=True)

        self._ticker_cik_map_path = os.path.join(self.path, 'ticker.json')
        self._ticker_cik_map = json.load(open(self._ticker_cik_map_path)) if os.path.isfile(self._ticker_cik_map_path) else {}

        self._company_cik_map_path = os.path.join(self.path, 'company.json')
        self._company_cik_map = json.load(open(self._company_cik_map_path)) if os.path.isfile(self._company_cik_map_path) else {}

    def build(self, n=8):
        now = datetime.now()
        summary_path = os.path.join(self.path, 'summary.json')
        company_zip = os.path.join(self.path, 'company_facts.zip')
        submission_zip = os.path.join(self.path, 'submissions.zip')

        if os.path.isfile(summary_path):
            last_summary = json.load(open(summary_path))
            os.rename(summary_path, os.path.join(self._summaries, f'''summary_{last_summary['build_timestamp']}.json'''))

        process_zip(COMPANY_FACTS_URL, company_zip, self._company_facts)
        process_zip(SUBMISSION_URL, submission_zip, self._submissions)

        file_names = [os.path.join(self._submissions, file_name) for file_name in os.listdir(self._submissions) if '.json' in file_name]
        chunk_size = len(file_names) // n
        with concurrent.futures.ProcessPoolExecutor(n) as pool:
            futures = [pool.submit(gather_index, file_names[i:i + chunk_size]) for i in range(0, len(file_names), chunk_size)]
            for future in concurrent.futures.as_completed(futures):
                t, c = future.result()
                self._ticker_cik_map.update(t)
                self._company_cik_map.update(c)

        with open(self._ticker_cik_map_path, 'w+') as ticker_cik_file:
            ticker_cik_file.write(json.dumps(self._ticker_cik_map, indent=4))

        with open(self._company_cik_map_path, 'w+') as company_name_cik_file:
            company_name_cik_file.write(json.dumps(self._company_cik_map, indent=4))

        with open(summary_path, 'w+') as summary_file:
            summary = {
                'build_timestamp': now.isoformat(),
                'total_size': sum(d.stat().st_size for d in os.scandir(self.path) if d.is_file()),
                'company_facts': {
                    # 'size': sum(d.stat().st_size for d in os.scandir(self._company_facts) if d.is_file()),
                    'files': len(os.listdir(self._company_facts)),
                    'zip_size': os.path.getsize(company_zip),
                },
                'submissions': {
                    # 'size': sum(d.stat().st_size for d in tqdm(os.scandir(self._submissions)) if d.is_file()),
                    'files': len(os.listdir(self._submissions)),
                    'zip_size': os.path.getsize(submission_zip),
                }
            }
            summary_file.write(json.dumps(summary, indent=4))

    def lookup_cik(self, ticker=None, company_name=None):
        if ticker and ticker in self._ticker_cik_map:
            return self._ticker_cik_map[ticker]
        if company_name and company_name in self._company_cik_map:
            return self._company_cik_map[company_name]

    def get_data(self, cik=None, ticker=None, company_name=None):
        if not cik:
            cik = self.lookup_cik(ticker=ticker, company_name=company_name)

        facts_path = os.path.join(self._company_facts, f'CIK{cik}.json')
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
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)

    if len(os.listdir(folder_path)) <= 0:
        with ZipFile(zip_path, 'r') as zip_file:
            zip_file.extractall(folder_path)


def gather_index(files):
    ticker_map = {}
    company_map = {}

    for file_path in files:
        with open(file_path) as submission_file:
            try:
                submission = json.load(submission_file)
                cik = submission['cik']
                cik = '0' * (10 - len(cik)) + cik

                company_map[submission['name']] = cik
                ticker_map.update({ticker: cik for ticker in submission['tickers']})
            except:
                pass

    return ticker_map, company_map
