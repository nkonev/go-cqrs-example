.PHONY: test
test:
	go test ./... -count=1 -test.v -test.timeout=60s -p 1
