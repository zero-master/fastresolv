# build stage
FROM golang:alpine AS build-env
ADD . /src
RUN apk add --no-cache git 
RUN go get github.com/garyburd/redigo/redis && go get -u github.com/gorilla/mux
RUN cd /src && go build -o goapp

# final stage
FROM alpine
WORKDIR /app
COPY --from=build-env /src/goapp /app/
EXPOSE 8080
ENTRYPOINT ./goapp