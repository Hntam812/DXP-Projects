# Use an official Python runtime as a parent image
FROM python:3.8

# Set the working directory in the container
WORKDIR /app

# Create directories
RUN mkdir /app/temp /app/logging

# Copy the local files to the container
COPY . .

# Create a directory for GPG keys and copy them
RUN mkdir /app/gpg_keys
COPY dsa_relationshop(06E074AE)-secret.asc /app/gpg_keys/key.asc
COPY passphrase.txt /app/gpg_keys/passphrase.txt
RUN echo /app/gpg_keys/passphrase.txt | gpg --batch --yes --import /app/gpg_keys/key.asc
# Install dependencies
RUN apt-get update && \
    pip install --upgrade pip && \
    apt-get install -y gnupg && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    pip install azure-storage-blob && \
    pip install python-decouple  && \
    pip install python-gnupg

# Run command
CMD ["python3", "./source/main.py"]
