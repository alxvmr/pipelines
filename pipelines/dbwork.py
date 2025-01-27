import psycopg2
from psycopg2 import Error
import pandas as pd
from .config import POSTGRES_PASSWORD, POSTGRES_USER, POSTGRES_DB, POSTGRES_HOST


class WorkWithDB:
    def __init__(self, dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD, host=POSTGRES_HOST, port='5432'):
        try:
            self.conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
            self.cursor = None
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)

    def run_query_without_output(self, query):
        try:
            self.cursor = self.conn.cursor()
            self.cursor.execute(query)
            self.conn.commit()
            self.cursor.close()
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)

    def number_of_records_after_query(self, query):
        try:
            self.cursor = self.conn.cursor()
            self.cursor.execute(query)
            cnt = len(self.cursor.fetchall())
            self.conn.commit()
            self.cursor.close()

            return cnt
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)

    def copy(self, query, file, t='r'):
        if t == 'r':
            data = open(file, 'r')
        else:
            data = open(file, 'w+')

        try:
            self.cursor = self.conn.cursor()

            self.cursor.copy_expert(query, data)
            self.conn.commit()

            self.cursor.close()
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)
        finally:
            data.close()

    def copy_to_table(self, input_file, table_name, sep=','):
        self.copy(f"COPY {table_name} FROM STDIN WITH CSV HEADER DELIMITER '{sep}'", input_file, t='r')

    def copy_to_file(self, table_name, output_file, sep=','):
        self.copy(f"COPY (SELECT * from {table_name}) TO STDOUT WITH CSV DELIMITER '{sep}' HEADER;", output_file, t='w')

    @staticmethod
    def db_data_type(data_type):
        conversion_table = {
            "int64": 'int8',  # 0 to 4924967295   # -9223372036854775808 to 9223372036854775807
            "object": 'text',
        }
        return conversion_table[data_type]

    def create_table_from_csv(self, input_file, table_name):  # создание таблицы с заголовком как в csv
        input_df = pd.read_csv(input_file, sep=',')
        header = dict(input_df.dtypes)
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ("

        for name, t in header.items():
            type_postgresql = self.db_data_type(str(t))
            query += f"{name} {type_postgresql}, "

        query = query[:-2] + ")"

        self.run_query_without_output(query)

    def create_table_as(self, name, sql_query):
        query = f"""
                    CREATE TABLE IF NOT EXISTS {name} as {sql_query}
                """
        self.run_query_without_output(query)

    def create_fun_domain_of_url(self):
        self.run_query_without_output("drop function if exists domain_of_url;")

        query = """
                create function domain_of_url(url text)
                returns text
                language plpgsql
                as
                $$
                declare
                   domain_of_url text;
                begin
                   select (regexp_matches(url, '\/\/(.*?)\/', 'g'))[1]
                   into domain_of_url;

                   return domain_of_url;
                end;
                $$;
                """
        self.run_query_without_output(query)
