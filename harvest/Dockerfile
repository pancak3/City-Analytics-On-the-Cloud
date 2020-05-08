FROM pypy:3-7-buster

# Copy app
COPY . /app
WORKDIR /app

# Install deps
RUN pip3 install --no-cache-dir -r requirements.txt

ENV http_proxy ''
ENV HTTP_PROXY ''

# Command
ENTRYPOINT [ "pypy3", "harvest.py" ]
