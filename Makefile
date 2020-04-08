.PHONY: FORCE


build: FORCE 
	go build -o ./build/iterum-sidecar


link: FORCE 
	@echo "Trying to link the executable to your path:"
	sudo ln -fs "${PWD}/build/iterum-sidecar" /usr/bin/iterum-sidecar
	@echo "Use iterum-sidercar to run"

clean: FORCE
	sudo rm /usr/bin/iterum-sidecar
	
image-sidecar:
	docker build -t iterum-sidecar:1 .
	kind load docker-image iterum-sidecar:1 --name kind