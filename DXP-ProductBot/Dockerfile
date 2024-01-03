FROM python:3.8

RUN apt-get update
RUN apt-get install default-jdk -y

#Timezone for UTC-5
RUN apt-get update && \
    apt-get install -yq tzdata wget && \
    ln -fs /usr/share/zoneinfo/America/New_York /etc/localtime && \
    dpkg-reconfigure -f noninteractive tzdata

WORKDIR /app

RUN mkdir /app/data
COPY product-bot /app
RUN pip install -r requirements.txt
EXPOSE 5000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "5000"]
