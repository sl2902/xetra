FROM python:3.8-slim-buster

COPY poetry.lock .
COPY pyproject.toml .

RUN pip install poetry --trusted-host pypi.python.org --no-cache-dir
RUN poetry config virtualenvs.create false
RUN poetry install --no-root --without dev


RUN mkdir scripts
copy scripts/ scripts

RUN mkdir config
COPY config/ config

RUN mkdir -p dbt/xetra
COPY dbt/xetra dbt/xetra