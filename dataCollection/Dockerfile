FROM python:3.8-slim-buster

WORKDIR /python-docker

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
RUN python3 -m nltk.downloader vader_lexicon
EXPOSE 5001
COPY . /python-docker
ENTRYPOINT [ "python3" ]
CMD [ "./src/dataCol.py" ]