FROM python:2.7-slim

WORKDIR /usr/src/app
COPY . .
COPY ./dot_libcchdo /root/.libcchdo

RUN apt-get update \
  && apt-get install -y build-essential python2-dev git \
  && pip install --no-python-version-warning --no-cache-dir -r requirements.txt \
  && pip install --no-python-version-warning --no-cache-dir . \
  && apt-get autoremove -y build-essential python2-dev && rm -rf /var/lib/apt/lists/*

VOLUME /context
WORKDIR /context

ENTRYPOINT ["hydro"]
