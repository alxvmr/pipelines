import pytest
from pipelines.dbwork import WorkWithDB


@pytest.fixture(scope="session", autouse=True)
def client():
    client = WorkWithDB()
    return client


@pytest.fixture(scope="function", autouse=True)
def drop_temp_table(client):
    query_clear = "DROP TABLE IF EXISTS temp"
    yield
    client.run_query_without_output(query_clear)  # удаляем временную таблицу после теста


@pytest.fixture(scope="function", autouse=False)
def create_table_for_CopyToFile(client):
    query = """
        CREATE TABLE temp (
        id int NOT NULL,
        name VARCHAR (100) NOT NULL,
        number int NOT NULL
        )
    """
    client.run_query_without_output(query)
    query = """
        INSERT INTO temp (id, name, number)
        VALUES
        (1, 'name1', 123),
        (2, 'name2', 435),
        (3, 'name3', 777)
    """
    client.run_query_without_output(query)


@pytest.fixture(scope="function", autouse=False)
def create_table_for_ctas(client):
    query = """
        CREATE TABLE temp (
        id int NOT NULL,
        name VARCHAR (100) NOT NULL,
        url VARCHAR (100) NOT NULL
        )
    """
    client.run_query_without_output(query)
    query = """
        INSERT INTO temp (id, name, url)
        VALUES
        (1, 'hello', 'http://hello.com/home'),
        (2, 'world', 'https://world.org/')
    """
    client.run_query_without_output(query)