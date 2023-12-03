FROM python:3.10-alpine
# FROM python:3.10-slim

RUN mkdir -p /crawler
WORKDIR /crawler

RUN pip install --upgrade pip
#RUN apt-get install python3-dev build-essential

COPY requirements.txt ./

RUN pip install -r requirements.txt

COPY . .

# ENTRYPOINT ["source","/crawler/init_queue.sh"]
