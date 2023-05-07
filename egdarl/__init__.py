import json
import os
import traceback
from datetime import datetime
from zipfile import ZipFile

import requests

__all__ = ['EDGAR', 'HEADERS']

COMPANY_FACTS_URL = 'https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip'
SUBMISSION_URL = 'https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip'

HEADERS = {"User-Agent": "Mozilla/5.0"}


class EDGAR:
    def __init__(self, dir: str):
        self.dir = dir
        self._company_facts = os.path.join(self.dir, 'company_facts')
        self._submissions = os.path.join(self.dir, 'submissions')
        self._summaries = os.path.join(self.dir, 'summaries')

        for dir in [self.dir, self._company_facts, self._submissions, self._summaries]:
            os.makedirs(dir, exist_ok=True)

        self._ticker_cik_map_path = os.path.join(self.dir, 'ticker.json')
        self._ticker_cik_map = json.load(open(self._ticker_cik_map_path)) if os.path.isfile(self._ticker_cik_map_path) else {}

        self._company_cik_map_path = os.path.join(self.dir, 'ticker.json')
        self._company_cik_map = json.load(open(self._company_cik_map_path)) if os.path.isfile(self._company_cik_map_path) else {}

    def build(self):
        now = datetime.now()
        summary_path = os.path.join(self.dir, 'summary.json')
        company_zip = os.path.join(self.dir, 'company_facts.zip')
        submission_zip = os.path.join(self.dir, 'submissions.zip')

        if os.path.isfile(summary_path):
            last_summary = json.load(open(summary_path))
            os.rename(summary_path, os.path.join(self._summaries, f'''summary_{last_summary['build_timestamp']}.json'''))

        process_zip(COMPANY_FACTS_URL, company_zip, self._company_facts)
        process_zip(SUBMISSION_URL, submission_zip, self._submissions)

        for file_name in os.listdir(self._submissions):
            if '.json' in file_name:
                with open(os.path.join(self.dir, file_name), 'r') as file:
                    try:
                        submission = json.load(file)
                        cik = submission['cik']
                        cik = '0' * (10 - len(cik)) + cik

                        self._company_cik_map[submission['name']] = cik
                        self._ticker_cik_map.update({ticker: cik for ticker in submission['tickers']})
                    except:
                        traceback.print_exc()

        with open(self._ticker_cik_map_path, 'w+') as ticker_cik_file:
            ticker_cik_file.write(json.dumps(self._ticker_cik_map, indent=4))

        with open(self._company_cik_map_path, 'w+') as company_name_cik_file:
            company_name_cik_file.write(json.dumps(self._company_cik_map, indent=4))

        with open(summary_path, 'w+') as summary_file:
            summary = {
                'build_timestamp': now.isoformat(),
                'total_size': sum(d.stat().st_size for d in os.scandir(self.dir) if d.is_file()),
                'company_facts': {
                    'size': sum(d.stat().st_size for d in os.scandir(self._company_facts) if d.is_file()),
                    'files': len(os.listdir(self._company_facts)),
                    'zip_size': os.path.getsize(company_zip),
                },
                'submissions': {
                    'size': sum(d.stat().st_size for d in os.scandir(self._submissions) if d.is_file()),
                    'files': len(os.listdir(self._submissions)),
                    'zip_size': os.path.getsize(submission_zip),
                }
            }
            summary_file.write(json.dumps(summary, indent=4))


def process_zip(url, path, dir):
    with requests.get(url, headers=HEADERS, stream=True) as r:
        r.raise_for_status()
        with open(path, 'wb+') as file:
            for chunk in r.iter_content():
                file.write(chunk)

    if len(os.listdir(dir)) <= 0:
        with ZipFile(path, 'r') as zip:
            zip.extractall(dir)
