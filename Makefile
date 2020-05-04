.PHONY: FORCE

NAME=sidecar

build: FORCE 
	go build -o ./build/${NAME}


link: FORCE 
	@echo "Trying to link the executable to your path:"
	sudo ln -fs "${PWD}/build/${NAME}" /usr/bin/${NAME}
	@echo "Use ${NAME} to run"

clean: FORCE
	sudo rm /usr/bin/${NAME}
	
image:
	docker build -t ${NAME}:1 .

replace-iterum-go: FORCE
	go mod edit -replace=github.com/iterum-provenance/iterum-go=$(GOPATH)/src/github.com/iterum-provenance/iterum-go

revert-iterum-go: FORCE
	go mod edit -dropreplace=github.com/iterum-provenance/iterum-go
