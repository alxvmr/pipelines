import pytest
from pipelines.tasks import CopyToFile, LoadFile, RunSQL, CTAS
import os.path
import pandas as pd
from data.pipeline import pipeline


@pytest.mark.parametrize("table, output_file, correct_file", [("temp", "data\\temp_output.csv", "data\\correct_load_file.csv")])
def test_copy_to_file(create_table_for_CopyToFile, table, output_file, correct_file):
    task = CopyToFile(table, output_file)
    task.run()
    # проверяем, что файл создался
    assert os.path.exists(output_file)
    # проверяем, что значения совпадают
    out_f = pd.read_csv(output_file, sep=',')
    cor_f = pd.read_csv(correct_file, sep=',')
    dif = out_f == cor_f
    assert not (False in dif.values.reshape(-1))
    os.remove(output_file)

@pytest.mark.parametrize("table, input_file", [("temp", "data\\correct_load_file.csv")])
def test_load_file(input_file, table, client):
    task = LoadFile(table, input_file)
    task.run()
    # проверим, что таблица создалась
    query = f"select * from pg_tables where tablename = '{table}'"
    assert client.number_of_records_after_query(query) == 1
    # проверим записи в таблице
    out_table_file = "data\\temp_output.csv"
    client.copy_to_file(table, out_table_file)
    out_f = pd.read_csv(out_table_file, sep=',')
    cor_f = pd.read_csv(input_file, sep=',')
    dif = out_f == cor_f
    assert not (False in dif.values.reshape(-1))
    os.remove(out_table_file)

@pytest.mark.parametrize("query, table", [("CREATE TABLE temp (id int)", "temp")])
def test_run_sql(query, table, client):
    task = RunSQL(query)
    task.run()
    # проверим, что запрос выполнился (создалась таблица)
    query = f"select * from pg_tables where tablename = '{table}'"
    assert client.number_of_records_after_query(query) == 1

@pytest.mark.parametrize("table1, table2, query", [("temp", "temp1", "select *, domain_of_url(url) from temp;")])
def test_ctas(table1, table2, query, create_table_for_ctas, client):
    task = CTAS(table2, query)
    task.run()
    # проверим, что таблица создалась
    query = f"select * from pg_tables where tablename = '{table2}'"
    assert client.number_of_records_after_query(query) == 1
    # проверим, что созданная таблица с таким же содержанием как у правильной
    out_table_file = "data\\temp1_output.csv"
    table_cor = "data\\correct_ctas.csv"
    client.copy_to_file(table2, out_table_file)
    f1 = pd.read_csv(out_table_file, sep=',')
    f2 = pd.read_csv(table_cor, sep=',')
    dif = f1 == f2
    assert not (False in dif.values.reshape(-1))
    # удаление второй таблицы
    query_clear = f"DROP TABLE IF EXISTS {table2}"
    client.run_query_without_output(query_clear)
    os.remove(out_table_file)

# проверим работу всего pipeline
def test_pipeline():
    pipeline.run()
    output_file = "data/norm.csv"
    cor_file = "data/correct_ctas.csv"
    f1 = pd.read_csv(output_file, sep=',')
    f2 = pd.read_csv(cor_file, sep=',')
    dif = f1 == f2
    assert not (False in dif.values.reshape(-1))

