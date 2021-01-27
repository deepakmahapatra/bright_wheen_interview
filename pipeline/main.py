import requests
import pandas as pd
from bs4 import BeautifulSoup

from constants import HEADERS
from provider_insertion_service import DataTransformationService

WEB_URL = 'http://naccrrapps.naccrra.org/navy/directory/programs.php?program=omcc&state=CA'
API_URL = 'https://bw-interviews.herokuapp.com/data/providers'


class DataCollector:
    def __init__(self):
        self.insertion_svc = DataTransformationService()

    def get_data_from_web(self):
        for idx in range(50):
            page = requests.get(WEB_URL + '&pagenum=' + str(idx))
            soup = BeautifulSoup(page.content, 'html.parser')
            table = soup.find('table')
            headers = [header.text for header in table.find_all('th')]
            rows = table.find_all('td')
            result = []

            for iteration in range(0, len(rows), len(headers)):
                row = rows[iteration:iteration+len(headers)]
                result.append([item.text for item in row])
            self.insertion_svc.insert_into_db(result, headers, "web")

    def get_data_from_csv(self, filename):
        chunksize = 100
        for chunk in pd.read_csv(filename, chunksize=chunksize, names=HEADERS[:-1]):
            self.insertion_svc.insert_into_db(chunk, HEADERS[: len(chunk.columns)], "file")

    def get_data_from_api(self):
        data = requests.get(API_URL)
        res = data.json().get("providers")
        headers = []
        if res:
            headers = res[0].keys()
        self.insertion_svc.insert_into_db(res, headers, "api")

    def get_question_querie_answers(self):
        first = self.insertion_svc.get_num_providers()
        second = self.insertion_svc.get_max_providers_zip()
        print("The total number of providers: ", first)
        print("The max number of providers are in zip: ", second)
        with open("./result/answer.txt", "w") as f:
            f.write("The total number of providers: {}\n".format(first))
            f.write("The max number of providers are in zip: {}\n".format(second))
            f.close()


if __name__ == '__main__':
    Wrangler = DataCollector()
    Wrangler.get_data_from_web()
    Wrangler.get_data_from_csv("x_ca_omcc_providers.csv")
    Wrangler.get_data_from_api()
    Wrangler.get_question_querie_answers()


