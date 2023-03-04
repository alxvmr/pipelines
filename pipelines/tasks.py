import os
from .dbwork import WorkWithDB

DB = WorkWithDB()


class BaseTask:
    """Base Pipeline Task"""

    def run(self):
        raise RuntimeError('Do not run BaseTask!')

    def short_description(self):
        pass

    def __str__(self):
        task_type = self.__class__.__name__
        return f'{task_type}: {self.short_description()}'


class CopyToFile(BaseTask):
    """Copy table data to CSV file"""

    def __init__(self, table, output_file):
        self.table = table

        # проверим, указали ли расширение csv
        if output_file.rsplit('.', 1)[-1] == "csv":
            self.output_file = os.path.join(os.path.abspath('data'), output_file)
        else:
            self.output_file = os.path.join(os.path.abspath('data'), f"{output_file}.csv")

    def short_description(self):
        return f'{self.table} -> {self.output_file}'

    def run(self):
        DB.copy_to_file(self.table, self.output_file)

        print(f"Copy table `{self.table}` to file `{self.output_file}`")


class LoadFile(BaseTask):
    """Load file to table"""

    def __init__(self, table, input_file):
        self.table = table
        self.input_file = input_file

    def short_description(self):
        return f'{self.input_file} -> {self.table}'


    def run(self):
        DB.create_table_from_csv(self.input_file, self.table) # создание таблицы
        DB.copy_to_table(self.input_file, self.table) # перенос данных в таблицу

        print(f"Load file `{self.input_file}` to table `{self.table}`")


class RunSQL(BaseTask):
    """Run custom SQL query"""

    def __init__(self, sql_query, title=None):
        self.title = title
        self.sql_query = sql_query

    def short_description(self):
        return f'{self.title}'

    def run(self):
        DB.run_query_without_output(self.sql_query)

        print(f"Run SQL ({self.title}):\n{self.sql_query}")


class CTAS(BaseTask):
    """SQL Create Table As Task"""

    def __init__(self, table, sql_query, title=None):
        self.table = table
        self.sql_query = sql_query
        self.title = title or table

    def short_description(self):
        return f'{self.title}'

    def run(self):
        """
        Была создана функция в БД
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

        query = f"""
            CREATE TABLE {self.table} as {self.sql_query}
        """
        DB.run_query_without_output(query)

        print(f"Create table `{self.table}` as SELECT:\n{self.sql_query}")
