#!/bin/bash

# setup logging
curl -sSO https://dl.google.com/cloudagents/install-logging-agent.sh sudo bash install-logging-agent.sh

# install system req
sudo apt-get update -y
sudo apt-get upgrade -y
sudo apt-get install -y \
    build-essential \
    curl \
    zlib1g-dev \
    libffi-dev \
    libssl-dev \
    libbz2-dev \
    libsqlite3-dev

# Download and compile Python 3.8.3 from source
wget https://www.python.org/ftp/python/3.8.3/Python-3.8.3.tgz
tar xzf Python-3.8.3.tgz
cd Python-3.8.3
./configure --enable-optimizations
make -j "$(nproc)"
sudo make altinstall
export PATH="/usr/local/lib/bin:$PATH"

# clone repo
cd /
git clone --depth 1 --filter=blob:none --sparse https://github.com/sl2902/data-engineering-zoomcamp.git;
cd data-engineering-zoomcamp;
git sparse-checkout set project
cd project

# create venv
python3.8.3 -m venv project-env
source project-env/bin/activate

# install poetry and dependencies
curl -sSL https://install.python-poetry.org | POETRY_HOME=/usr/local/lib python3.8.3 -
poetry install --no-root --without dev,flows

# set env vars
export PREFECT_API_KEY=`curl  -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/PREFECT_API_KEY"`
export PREFECT_API_URL=`curl  -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/PREFECT_API_URL"`
export PREFECT_AGENT_QUEUE_NAME=`curl  -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/PREFECT_AGENT_QUEUE_NAME"`

prefect agent start -q $PREFECT_AGENT_QUEUE_NAME