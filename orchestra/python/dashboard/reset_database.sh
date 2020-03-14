docker stop postgres && docker rm postgres
docker run -it -d --restart unless-stopped --name postgres -p 5432:5432 postgres
