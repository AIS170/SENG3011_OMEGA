FROM python:3.12.0b1-slim-buster

WORKDIR /python-retrievalFunction
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
COPY . /python-retrievalFunction
ENTRYPOINT [ "python3" ]
CMD [ "./implementation/RetrievalMicroservice.py" ]
