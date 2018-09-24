FROM python:2.7

RUN apt-get update && apt-get install -y libnetcdf-dev

WORKDIR /usr/src/app
COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt
COPY . .

COPY ./dot_libcchdo /root/.libcchdo

RUN pip install .

VOLUME /context
WORKDIR /context

ENTRYPOINT ["hydro"]
