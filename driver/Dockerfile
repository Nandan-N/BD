FROM golang:1.19

WORKDIR /app

COPY ../go.mod ../go.sum ./

COPY ../lib/dlts.go ./lib/

RUN go mod download

COPY driver/main.go ./

RUN CGO_ENABLED=0 GOOS=linux go build -o /mini-driver

CMD ["/mini-driver"]
