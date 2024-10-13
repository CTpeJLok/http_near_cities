FROM python:3.11.9-slim-bullseye

RUN apt update
RUN apt install -y netcat

RUN pip install -U pip

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


WORKDIR /app

COPY ./*.py ./
COPY ./entrypoint.sh .

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1


ENTRYPOINT [ "/app/entrypoint.sh" ]
