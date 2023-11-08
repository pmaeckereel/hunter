import uuid
import logging
import psycopg2
import psycopg2.extras

from csv import DictReader
from datetime import datetime
from typing import Dict, List
from abc import ABC, abstractmethod

class Ingester(ABC):
    def __init__(self, dataset_path: str):
        self.dataset_path = dataset_path
        self.raw_data = []
        psycopg2.extras.register_uuid() ## Required to be able to ingest UUIDs into postgres.

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
        Additional checks could be done here (ex: columns existence, format, type, country_code exists, etc).

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
        types_to_ensure = {
            "num_of_employees": int,
            "revenue": float,
            "profit": float,
            "market_cap": float
        }

        with open(self.dataset_path) as fr:
            reader = DictReader(fr, delimiter=";")
            for row in reader:
                temp_row = {k:v for k, v in row.items() if k not in keys_to_remove}
                temp_row = {keys_to_rename[k] if k in keys_to_rename.keys() else k:v for k,v in temp_row.items()}
                temp_row["uuid"] = uuid.uuid4() # should not be done there, but rather have a global mapping with proper ids.
                for k, v in types_to_ensure.items():
                    if v == int:
                        temp_row[k] = int(float(temp_row[k])) if temp_row[k] != '' else None ## ensure type to int.
                    if v == float:
                        temp_row[k] = float(temp_row[k]) if temp_row[k] not in ['', '-'] else None ## ensure type to float.
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
            "revenue", "profit", "profitable", "ticker", "market_cap"]

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

        sql_query_kaggle = """INSERT INTO kaggle (company, num_of_employees, sector, city, state, newcomer, 
            ceo_founder, ceo_woman, ceo, website, uuid, updated_at, created_at)
             VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""

        sql_query_kaggle_financial_infos = """INSERT INTO kaggle_financial_infos (revenue, 
            profit, profitable, ticker, market_cap, uuid, updated_at, created_at)
             VALUES(%s, %s, %s, %s, %s, %s, %s, %s);"""

        try:
            connection = psycopg2.connect("dbname=sources")
            cursor = connection.cursor()
            for data in tables_data[0]:
                cursor.execute(sql_query_kaggle, tuple(data.values()))
            for data in tables_data[1]:
                cursor.execute(sql_query_kaggle_financial_infos, tuple(data.values()))
            connection.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            logging.error(error)
        finally:
            if connection is not None:
                connection.close()

class CrunchbaseIngester(Ingester):
    def load_data(self):
        """
        Get data from csv into dict.
        Don't get columns we won't use, and clean some names.
        Additional checks could be done here (ex: columns existence, format, type).

        Data is not yet separated between tables.
        """
        keys_to_remove = ["type", "cb_url", "rank", "homepage_url", "primary_role", "num_exits", "revenue_range"]
        keys_to_rename = {
            "country_code": "country",
            "created_at": "crunchbase_created_at",
            "updated_at": "crunchbase_updated_at"
        }
        types_to_ensure = {
            "founded_on": "date"
        }

        with open(self.dataset_path) as fr:
            reader = DictReader(fr, delimiter=",")
            for row in reader:
                temp_row = {k:v for k, v in row.items() if k not in keys_to_remove}
                temp_row = {keys_to_rename[k] if k in keys_to_rename.keys() else k:v for k,v in temp_row.items()}
                temp_row["uuid"] = uuid.uuid4() # should not be done there, but rather have a global mapping with proper ids.
                for k, v in types_to_ensure.items():
                    if v == int:
                        temp_row[k] = int(float(temp_row[k])) if temp_row[k] != '' else None ## ensure type to int.
                    if v == float:
                        temp_row[k] = float(temp_row[k]) if temp_row[k] not in ['', '-'] else None ## ensure type to float.
                    if v == "date":
                        temp_row[k] = None if temp_row[k] == "" else temp_row[k]
                self.raw_data.append(temp_row)

    def prepare_table_data(self) -> List[List[Dict]]:
        """
        Seperate the data bewteen the different tables we'll use.
        Hunter tables are 'crunchbase', 'crunchbase_contact', 'crunchbase_categories', 'crunchbase_description',
        'crunchbase_category_groups', 'crunchbase_aliases', 'crunchbase_roles', 'crunchbase_funding'.
        Schemas can be found in sql_scripts/crunchbase.sql

        Returns:
            List[List[Dict]]: one dataset (list of dict) by table.
        """
        crunchbase_table_data = []
        crunchbase_contact_table_data = []
        crunchbase_categories_table_data = []
        crunchbase_description_table_data = []
        crunchbase_category_groups_table_data = []
        crunchbase_aliases_table_data = []
        crunchbase_roles_table_data = []
        crunchbase_funding = []

        crunchbase_columns = ["id", "uuid", "domain", "name", "permalink", "crunchbase_created_at", 
            "crunchbase_updated_at", "country", "status", "founded_on", "employee_count"]
        crunchbase_contact_columns = ["uuid", "email", "phone", "facebook_url",
            "linkedin_url", "twitter_url", "region", "city", "address",
            "postal_code"]
        crunchbase_description_columns = ["uuid", "short_description"]
        crunchbase_funding_columns = ["uuid", "num_funding_rounds", "total_funding_usd", "total_funding",
            "total_funding_currency_code", "last_funding_on", "closed_on"]

        creation_time = datetime.now()
        for e in self.raw_data:
            crunchbase_table_data.append({k: v for k, v in e.items() if k in crunchbase_columns})
            crunchbase_contact_table_data.append(
                {k: v for k, v in e.items() if k in crunchbase_contact_columns})
            crunchbase_description_table_data.append({k: v for k, v in e.items() if k in crunchbase_description_columns})
            categories = e["category_list"].split(',')
            for c in categories:
                crunchbase_categories_table_data.append({"uuid": e["uuid"], "category": c})
            category_groups = e["category_groups_list"].split(',')
            for c in categories:
                crunchbase_category_groups_table_data.append({"uuid": e["uuid"], "category_group": c})
            aliases = [e["legal_name"], e["alias1"], e["alias2"], e["alias3"]]
            ## Cleaning up empty and duplicated aliases.
            aliases = [a.lower() for a in aliases if a != '']
            aliases = list(set(aliases))
            for a in aliases:
                crunchbase_aliases_table_data.append({"uuid": e["uuid"], "alias": a})
            roles = e["roles"].split(',')
            for r in roles:
                crunchbase_roles_table_data.append({"uuid": e["uuid"], "role": r})

        returned_datas = [crunchbase_table_data, crunchbase_contact_table_data, 
            crunchbase_categories_table_data, crunchbase_description_table_data, crunchbase_category_groups_table_data,
            crunchbase_aliases_table_data, crunchbase_roles_table_data]
        for table_data in returned_datas:
            for d in table_data:
                d["updated_at"] = creation_time
                d["created_at"] = creation_time

        return returned_datas

    def ingest_data(self, tables_data: List[List[Dict]]) -> None:
        """
        Ingest prepared data into tables 'crunchbase', 'crunchbase_contact', 'crunchbase_categories',
        'crunchbase_description', 'crunchbase_category_groups', 'crunchbase_aliases' and 'crunchbase_roles'.
        A postgresql server must be setup beforehand on the same machine this script is run.
        """

        sql_query_crunchbase = """INSERT INTO crunchbase (id, uuid, domain, name, permalink, crunchbase_created_at,
            crunchbase_updated_at, country, status, founded_on, employee_count,
            created_at, updated_at)
             VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""

        sql_query_crunchbase_contact = """INSERT INTO crunchbase_contact (uuid, email, phone, facebook_url,
            linkedin_url, twitter_url, region, city, address, postal_code, created_at, updated_at)
             VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""

        sql_query_crunchbase_description = """INSERT INTO crunchbase_description (uuid, short_description, 
            created_at, updated_at)
             VALUES(%s, %s, %s, %s);"""

        sql_query_crunchbase_categories = """INSERT INTO crunchbase_categories (uuid, category, created_at, updated_at)
             VALUES(%s, %s, %s, %s);"""

        sql_query_crunchbase_category_groups = """INSERT INTO crunchbase_category_groups (uuid, category_group, 
            created_at, updated_at)
             VALUES(%s, %s, %s, %s);"""

        sql_query_crunchbase_aliases = """INSERT INTO crunchbase_aliases (uuid, alias, created_at, updated_at)
             VALUES(%s, %s, %s, %s);"""

        sql_query_crunchbase_roles = """INSERT INTO crunchbase_roles (uuid, role, created_at, updated_at)
             VALUES(%s, %s, %s, %s);"""

        sql_queries = [sql_query_crunchbase, sql_query_crunchbase_contact, sql_query_crunchbase_categories,
            sql_query_crunchbase_description, sql_query_crunchbase_category_groups, sql_query_crunchbase_aliases,
            sql_query_crunchbase_roles]

        try:
            connection = psycopg2.connect("dbname=sources")
            cursor = connection.cursor()
            for i, query in enumerate(sql_queries):
                for data in tables_data[i]:
                    cursor.execute(query, tuple(data.values()))
            connection.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            logging.error(error)
        finally:
            if connection is not None:
                connection.close()

class HunterIngester(Ingester):
    def load_data(self):
        """
        Get data from csv into dict.
        Don't get columns we won't use, and clean some names.
        Additional checks could be done here (ex: columns existence, format, type).

        Data is not yet separated between tables.
        """
        keys_to_remove = []
        keys_to_rename = {
            "value": "domain",
            "twitter": "twitter_url",
            "facebook": "facebook_url",
            "instagram": "instagram_url",
            "youtube": "youtube_url",
            "linkedin": "linkedin_url",
            "phone_number": "phone"
        }
        types_to_ensure = {
            "employees_count": int,
            "generic_emails": int,
            "personal_emails": int,
            "last_crawl": "date"
        }

        with open(self.dataset_path) as fr:
            reader = DictReader(fr, delimiter=";")
            for row in reader:
                temp_row = {k:v for k, v in row.items() if k not in keys_to_remove}
                temp_row = {keys_to_rename[k] if k in keys_to_rename.keys() else k:v for k,v in temp_row.items()}
                temp_row["uuid"] = uuid.uuid4() # should not be done there, but rather have a global mapping with proper ids.
                for k, v in types_to_ensure.items():
                    if v == int:
                        temp_row[k] = int(float(temp_row[k])) if temp_row[k] != '' else None ## ensure type to int.
                    if v == float:
                        temp_row[k] = float(temp_row[k]) if temp_row[k] not in ['', '-'] else None ## ensure type to float.
                    if v == "date":
                        temp_row[k] = None if temp_row[k] == "" else temp_row[k]
                self.raw_data.append(temp_row)

    def prepare_table_data(self) -> List[List[Dict]]:
        """
        Seperate the data bewteen the different tables we'll use.
        Hunter tables are 'hunter', 'hunter_contact', 'hunter_categories' and 'hunter_description'.
        Schemas can be found in sql_scripts/hunter.sql

        Returns:
            List[List[Dict]]: one dataset (list of dict) by table.
        """
        hunter_table_data = []
        hunter_contact_table_data = []
        hunter_categories_table_data = []
        hunter_description_table_data = []

        hunter_columns = ["id", "domain", "generic_emails",
            "personal_emails", "last_crawl", "language", "company_name", "country", 
            "employees_count"]
        hunter_contact_columns = ["id", "twitter_url", "facebook_url", "instagram_url",
            "linkedin_url", "youtube_url", "phone", "apple_app", "google_play",
            "state", "city", "street", "postcode"]
        hunter_description_columns = ["id", "description"]

        creation_time = datetime.now()
        for e in self.raw_data:
            hunter_table_data.append({k: v for k, v in e.items() if k in hunter_columns})
            hunter_contact_table_data.append(
                {k: v for k, v in e.items() if k in hunter_contact_columns})
            hunter_description_table_data.append({k: v for k, v in e.items() if k in hunter_description_columns})
            categories = e["categories"].strip("{}").split(',')
            for c in categories:
                hunter_categories_table_data.append({"id": e["id"], "category": c})

        returned_datas = [hunter_table_data, hunter_contact_table_data, 
            hunter_categories_table_data, hunter_description_table_data]
        for table_data in returned_datas:
            for d in table_data:
                d["updated_at"] = creation_time
                d["created_at"] = creation_time

        return returned_datas

    def ingest_data(self, tables_data: List[List[Dict]]) -> None:
        """
        Ingest prepared data into tables 'hunter', 'hunter_contact', 'hunter_categories' and 'hunter_description'.
        A postgresql server must be setup beforehand on the same machine this script is run.
        """

        sql_query_hunter = """INSERT INTO hunter (id, domain, generic_emails, personal_emails, 
            last_crawl, language, company_name, country, employees_count, created_at, updated_at)
             VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""

        sql_query_hunter_contact = """INSERT INTO hunter_contact (id, twitter_url, facebook_url, 
            instagram_url, linkedin_url, youtube_url, phone, apple_app, google_play, state, city, street,
            postcode, created_at, updated_at)
             VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""

        sql_query_hunter_description = """INSERT INTO hunter_description (id, description, created_at, updated_at)
             VALUES(%s, %s, %s, %s);"""

        sql_query_hunter_categories = """INSERT INTO hunter_categories (id, category, created_at, updated_at)
             VALUES(%s, %s, %s, %s);"""

        try:
            connection = psycopg2.connect("dbname=sources")
            cursor = connection.cursor()
            for data in tables_data[0]:
                cursor.execute(sql_query_hunter, tuple(data.values()))
            for data in tables_data[1]:
                cursor.execute(sql_query_hunter_contact, tuple(data.values()))
            for data in tables_data[2]:
                cursor.execute(sql_query_hunter_categories, tuple(data.values()))
            for data in tables_data[3]:
                cursor.execute(sql_query_hunter_description, tuple(data.values()))
            connection.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            logging.error(error)
        finally:
            if connection is not None:
                connection.close()