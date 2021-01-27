from constants import HEADERS, HEADERS_TO_COL
from db_layer import PostgresConn


class DataTransformationService:
    template = {
        "provider_name": None,
        "type_of_care": None,
        "address": None,
        "city": None, "state": None, "zip": None, "phone": None, "email": None
    }

    def __init__(self):
        self.dal = PostgresConn()

    def insert_into_db(self, vendor_list, headers, type):
        transformed_rows = []
        if type == "api":
            self.dal.insert_owner_list(vendor_list)
            self.dal.update_vendor_owner(vendor_list)
        if type == "file":
            transformed_rows = [self.map_rows_df(row, headers) for row in vendor_list.iterrows()]
            self.dal.insert_vendor_list(transformed_rows)
        elif type == "web":
            transformed_rows = [self.map_rows_web(row, headers) for row in vendor_list]
            self.dal.insert_vendor_list(transformed_rows)

    def map_rows_df(self, row, headers):
        dict_ = self.template.copy()
        for header in HEADERS:
            if header in headers:
                dict_[HEADERS_TO_COL[header]] = row[1][header]
        return dict_

    def map_rows_web(self, row, headers):
        dict_ = self.template.copy()
        for header in HEADERS:
            if header in headers:
                dict_[HEADERS_TO_COL[header]] = row[headers.index(header)]
        return dict_

    def get_num_providers(self):
        res = self.dal.get_num_care_providers()
        return res[0]

    def get_max_providers_zip(self):
        return self.dal.get_max_providers_zip()[0]
