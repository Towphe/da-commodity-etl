
# stop container
docker-compose down

# delete image
docker image rm crop-price-etl_da

# delete network
docker network rm db-bridge
