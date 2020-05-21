# Clone cloudant
FROM alpine:latest as cloudant
RUN apk add git
RUN git clone https://github.com/cloudant/python-cloudant.git

# Build harvest image
FROM pypy:3-7-slim-buster

# Copy app
WORKDIR /app
COPY . /app

# Install deps
RUN pip3 install --no-cache-dir -r requirements.txt

# Install cloudant
COPY --from=cloudant /python-cloudant /python-cloudant
RUN pip3 install /python-cloudant && rm -rf /python-cloudant

# Do not run as root
RUN useradd -m comp90024
User comp90024

# Command
ENTRYPOINT [ "pypy3", "check_couchdb_for_api.py" ]
