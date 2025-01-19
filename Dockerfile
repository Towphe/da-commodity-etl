# use python 3.11 docker image
FROM python:3.11-alpine

# copy source file to /app
COPY ./ /app

# set working environment
WORKDIR /app

# set db key
ENV DB_KEY="postgresql://tope:pingu@db:5440/commodity_db"

# install python3 packages
RUN pip install --no-cache-dir --upgrade -r requirements.txt

# register as CRON job
# CMD []
CMD [ "python3", "src/main.py" ]
