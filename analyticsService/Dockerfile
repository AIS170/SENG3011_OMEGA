FROM python:3.12
WORKDIR /python-analyticsMicroservice
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
COPY . /python-analyticsMicroservice
ENTRYPOINT [ "python3" ]
CMD [ "analytics_api.py" ]
