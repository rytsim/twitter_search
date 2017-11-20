############################################################
# Dockerfile twitter_search
# Based on python:3
############################################################


FROM python:3

# Create dirs and add app files
RUN mkdir -p /data
RUN mkdir -p /app
WORKDIR /app
ADD . /app

# Install python requirements
RUN pip3 install --no-cache-dir -r  requirements.txt
RUN pip3 install --no-cache-dir git+https://github.com/rytsim/TwitterSearch.git

# On initialization run
ENTRYPOINT ["python", "twitter_search.py"]
# pass script argments trough CMD
CMD []


