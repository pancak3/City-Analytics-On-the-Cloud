# @author Team 42, Melbourne, Steven Tang, 832031

# Clone cloudant
FROM alpine:latest as cloudant
RUN apk add git
RUN git clone https://github.com/cloudant/python-cloudant.git

# Build harvest image
FROM pypy:3-7-slim-buster

# Install deps
COPY requirements.txt /requirements.txt
RUN apt-get update && apt-get -y install gcc && apt-get clean
RUN pip3 install --no-cache-dir -r requirements.txt
RUN apt-get -y remove gcc && apt-get clean

# Install cloudant
COPY --from=cloudant /python-cloudant /python-cloudant
RUN pip3 install /python-cloudant && rm -rf /python-cloudant

# Copy the rest
COPY . /app
WORKDIR /app

# Do not run as root
RUN useradd -m comp90024
User comp90024

# Command
ENTRYPOINT [ "pypy3", "harvest.py" ]
