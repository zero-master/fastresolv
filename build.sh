set -e
go get github.com/garyburd/redigo/redis
go get -u github.com/gorilla/mux
go build main.go