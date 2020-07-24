.PHONY: FORCE

NAME=iterum/sidecar
TAG=latest

image:
	docker build -t ${NAME}:${TAG} .

build: FORCE 
	go build -o ./build/${NAME}