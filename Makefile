gen:
	protoc --go_out=. proto/*

produce:
	go run cmd/producer/main.go

consume:
	go run cmd/consumer/main.go
