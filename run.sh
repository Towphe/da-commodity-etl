
# change access rights to data/ folder
sudo chown -R $USER data

# create docker network
docker network create db-bridge

# run program
docker-compose up
