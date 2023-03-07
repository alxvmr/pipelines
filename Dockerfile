FROM python:3.10.5-slim-buster as base

ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1
WORKDIR /pipelines
RUN pip install --upgrade pip 


FROM base as dep-poetry
ENV POETRY_HOME /opt/poetry
RUN python3 -m venv $POETRY_HOME
RUN $POETRY_HOME/bin/pip install poetry==1.2.2
ENV POETRY_BIN $POETRY_HOME/bin/poetry
COPY pyproject.toml poetry.lock ./
RUN $POETRY_BIN config --local virtualenvs.create false
RUN $POETRY_BIN install --no-root

COPY . .
RUN pip install .

#ADD https://github.com/ufoscout/docker-compose-wait/releases/download/2.7.3/wait /wait
#RUN chmod +x /wait
#CMD /wait && npm start

WORKDIR ./example_pipeline

CMD ["python", "./pipeline.py"]