FROM python:3.11.3-slim-bullseye

WORKDIR /bcfinder

ADD requirements.txt /bcfinder
RUN pip install -r requirements.txt

COPY . /bcfinder

CMD ["python", "-m", "main"]