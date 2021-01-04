FROM python:3.7.7-buster

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt && pip install prospector coverage nose2 twine