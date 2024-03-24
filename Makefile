PKG_LIST := $(shell go list ./... | grep -v /vendor/)
PATH := $(PATH):$(GOPATH)/bin

.PHONY: build
build:
	go build -o bin/links-srv cmd/links-srv/main.go
	go build -o bin/users-srv cmd/users-srv/main.go
	go build -o bin/users-srv cmd/api-gw/main.go

.PHONY: clean
clean:
	rm -rf bin/

.PHONY: lint
lint:
	golangci-lint run --timeout 5m -v ./...

.PHONY: drop-links
drop-links:
	go run tools/drop-links-db

.PHONY: gen-fake
gen-fake:
	go run tools/gen-fakery $(ARGS)

.PHONY: genid
genid:
	go run tools/genid/main.go

.PHONY: generate
generate:
	protoc --go_out=pkg/pb --go_opt=paths=source_relative --go-grpc_out=pkg/pb --go-grpc_opt=paths=source_relative \
	--proto_path=./pkg/pb ./pkg/pb/common.proto

	protoc --go_out=pkg/pb --go_opt=paths=source_relative --go-grpc_out=pkg/pb --go-grpc_opt=paths=source_relative \
	--proto_path=./pkg/pb ./pkg/pb/users.proto

	protoc --go_out=pkg/pb --go_opt=paths=source_relative --go-grpc_out=pkg/pb --go-grpc_opt=paths=source_relative \
	--proto_path=./pkg/pb ./pkg/pb/links.proto

	go generate ./...

.PHONY: install
install:
	go get google.golang.org/protobuf/cmd/protoc-gen-go
	go get google.golang.org/grpc/cmd/protoc-gen-go-grpc
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	go install github.com/deepmap/oapi-codegen/cmd/oapi-codegen@latest
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@1.56.2

.PHONY: test
test:
	go test -short ./...

.PHONY: integration
integration:
	go test -race ./...

.PHONY: docker
docker:
	docker run --name manager-pg -e POSTGRES_DB=users -e POSTGRES_USER=postgres -e \
	POSTGRES_PASSWORD=postgres -d -p "5434:5432" postgres
	docker run --name manager-mongo -d -p 27018:27017 mongo

.PHONY: migrate-up
migrate-up:
	 migrate -source "file://./migrations" -database "postgres://localhost:5434/users?sslmode=disable&user=postgres&password=postgres" up

.PHONY: migrate-down
migrate-down:
	 migrate -source "file://./migrations" -database \
	 "postgres://localhost:5434/users?sslmode=disable&user=postgres&password=postgres" down