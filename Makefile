all: bin/nectar

bin/nectar: *.go */*.go
	mkdir -p bin
	go build -o bin/nectar github.com/troubling/nectar/nectar

get:
	go get -t $(shell go list ./... | grep -v /vendor/)

fmt:
	gofmt -l -w -s $(shell find . -mindepth 1 -maxdepth 1 -type d -print | grep -v vendor)

test:
	@test -z "$(shell find . -name '*.go' | grep -v ./vendor/ | xargs gofmt -l -s)" || (echo "You need to run 'make fmt'"; exit 1)
	go vet $(shell go list ./... | grep -v /vendor/)
	go test -cover $(shell go list ./... | grep -v /vendor/)

haio: all
	sudo rm -f /usr/bin/nectar
	sudo cp bin/nectar /usr/bin/nectar
	sudo chmod 0755 /usr/bin/nectar
