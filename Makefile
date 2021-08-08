test:
	go test -vet=off -v $(shell go list ./...)
