############################################################
# Dockerfile temp
# Based on Ubuntu
############################################################


# Set the base image to Ubuntu
# FROM ubuntu
FROM python:3

# Create dirs and add app files
RUN mkdir -p /data
RUN mkdir -p /app
WORKDIR /app
ADD . /app


# Install python and requirements
# RUN apt-get update && apt-get -y upgrade 
# RUN apt-get update
# RUN apt-get install -y python3 python3-pip git
RUN pip3 install --no-cache-dir -r  requirements.txt
RUN pip3 install --no-cache-dir git+https://github.com/rytsim/TwitterSearch.git

# On initialization run
# CMD ["python3"]
# ENTRYPOINT ["python3 twitter_search.py"]
ENTRYPOINT ["python", "twitter_search.py"]
CMD []


