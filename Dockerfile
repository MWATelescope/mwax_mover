FROM python:3.11-slim-bookworm

# copy the repository into the container in the /app directory
ADD . /app
WORKDIR /app
RUN pip install --no-cache-dir -r requirements.txt

# install the python module in the container
RUN pip install .

ENTRYPOINT /bin/bash