build:
	docker build --network host --compress -t orchestra/base -f Dockerfile.base .
	docker build --network host --compress -t orchestra/web  -f Dockerfile.web .
	docker build --network host --compress -t orchestra/api  -f Dockerfile.api .
	docker build --network host --compress -t orchestra/main -f Dockerfile.main .
run:
	docker-compose -f container_stack.yml up	
