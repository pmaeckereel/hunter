import uuid

from datetime import datetime
from csv import DictReader
from typing import Dict, List
from abc import ABC, abstractmethod

class Ingester(ABC):
    def __init__(self, dataset_path: str):
        self.dataset_path = dataset_path
        self.raw_data = []

    @abstractmethod
    def load_data(self):
        pass

    @abstractmethod
    def prepare_table_data(self):
        pass

    @abstractmethod
    def ingest_data(self, tables_data: List[List[Dict]]):
        pass

class KaggleIngester(Ingester):
    def load_data(self):
        """
        Get data from csv into dict.
        Don't get columns we won't use, and clean some names.
        Additional checks could be done here (ex: columns existence, format, type).

        Data is not yet separated between tables.
        """
        keys_to_remove = ["rank", "rank_change", "prev_rank"]
        keys_to_rename = {
            "num. of employees": "num_of_employees",
            "CEO": "ceo",
            "Website": "website",
            "Ticker": "ticker",
            "Market Cap": "market_cap"
        }
        with open(self.dataset_path) as fr:
            reader = DictReader(fr, delimiter=";")
            for row in reader:
                temp_row = {k:v for k, v in row.items() if k not in keys_to_remove}
                temp_row = {keys_to_rename[k] if k in keys_to_rename.keys() else k:v for k,v in temp_row.items()}
                temp_row["uuid"] = uuid.uuid4() # should not be done there, but rather have a global mapping with proper ids.
                self.raw_data.append(temp_row)

    def prepare_table_data(self) -> List[List[Dict]]:
        """
        Seperate the data bewteen the different tables we'll use.
        Kaggle tables are 'kaggle' and 'kaggle_financial_infos'.
        Schemas can be found in sql_scripts/kaggle.sql

        Returns:
            List[List[Dict]]: one dataset (list of dict) by table.
        """
        kaggle_table_data = []
        kaggle_financial_infos_table_data = []
        kaggle_columns = ["uuid", "company", "num_of_employees",
            "sector", "city", "state", "newcomer", "ceo_founder", 
            "ceo_woman", "ceo", "website"]
        kaggle_financial_infos_columns = ["uuid",
            "revenue", "profit", "profitable", "ticker", "market"]

        creation_time = datetime.now()
        for e in self.raw_data:
            kaggle_table_data.append({k: v for k, v in e.items() if k in kaggle_columns})
            kaggle_financial_infos_table_data.append(
                {k: v for k, v in e.items() if k in kaggle_financial_infos_columns})

        for d in kaggle_table_data:
            d["updated_at"] = creation_time
            d["created_at"] = creation_time

        for d in kaggle_financial_infos_table_data:
            d["updated_at"] = creation_time
            d["created_at"] = creation_time

        return [kaggle_table_data, kaggle_financial_infos_table_data]

    def ingest_data(self, tables_data: List[List[Dict]]) -> None:
        """
        Ingest prepared data into tables 'kaggle' and 'kaggle_financial_infos'.
        A postgresql server must be setup beforehand on the same machine this script is run.
        """
        pass

class CrunchbaseIngester(Ingester):
    def load_data(self):
        with open(self.dataset_path) as fr:
            reader = DictReader(fr)
            for row in reader:
                print(row)
                break

class HunterIngester(Ingester):
    def load_data(self):
        with open(self.dataset_path) as fr:
            reader = DictReader(fr)
            for row in reader:
                print(row)
                break