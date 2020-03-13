docker stop postgres && docker rm postgres
docker run -it -d --restart unless-stopped --name postgres -p 5432:5432 postgres
python3 ../../scripts/create_lps_db.py
