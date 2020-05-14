FROM pypy:3-7-slim-buster

# Copy app
WORKDIR /app
COPY . /app

# Install deps
RUN pip3 install --no-cache-dir -r requirements.txt
RUN pip3 install git+https://github.com/cloudant/python-cloudant.git

# Command
ENTRYPOINT [ "pypy3", "harvest.py" ]
