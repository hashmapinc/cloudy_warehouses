FROM python:3.7.7-buster

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt && pip3 install bandit && pip install pydocstyle prospector coverage nose2 twine
