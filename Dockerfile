FROM python:3.5-alpine

COPY requirements.txt /pkg/requirements.txt

RUN pip install -r /pkg/requirements.txt

COPY index_cloner.py /index_cloner.py

ENTRYPOINT ["python", "/index_cloner.py"]
