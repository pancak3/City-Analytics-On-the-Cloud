FROM pypy:3-7-buster

# Copy app
COPY . /app
WORKDIR /app

# Install deps
RUN pip3 install --no-cache-dir -r requirements.txt

# Command
CMD [ "pypy3", "harvest.py" ]
