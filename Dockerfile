FROM python:3.8.1-slim

ENV PYTHONUNBUFFERED 1

RUN chmod g+rx,o+rx /

# Application packages
RUN apt-get update && apt-get install --assume-yes \
        vim \
        bash \
        gcc \
        libc6-dev \
        postgresql \
        libxml2-dev \
        libxslt-dev \
    && mkdir -p /app \
    && mkdir -p /app/requirements

# ENVIRONMENT - "prod" or "dev"
ARG PROJECT_ENVIRONMENT=dev

COPY env/requirements /app/requirements

# Build packages
RUN pip install --upgrade pip==19.3.1 \
    && pip install --no-cache-dir -r /app/requirements/${PROJECT_ENVIRONMENT}.txt

WORKDIR /app

# Copy project files
COPY src /app

# Copy dev tools configs
RUN mkdir /python
COPY .pylintrc /python
COPY mypy.ini /python

ENV PYTHONPATH="/app:${PATH}"
