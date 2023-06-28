FROM python:3.11.3-slim-bullseye as base

WORKDIR /bcfinder

COPY . /bcfinder

ADD requirements.txt /bcfinder
RUN pip install -r requirements.txt

COPY . /bcfinder

CMD ["python", "-m", "main"]

#########################
FROM base as test

#layer test tools and assets on top as optional test stage
RUN apt-get update && apt-get install -y curl apache2-utils

#########################
FROM base as final

# this layer gets built by default unless you set target to test