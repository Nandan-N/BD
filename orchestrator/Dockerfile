FROM golang:1.19

EXPOSE 8001

WORKDIR /app

COPY ../go.mod ../go.sum ./

COPY ../lib/dlts.go ./lib/

COPY orchestrator/static/ ./static/

COPY orchestrator/templates/ ./templates/

RUN go mod download

COPY orchestrator/main.go ./

RUN CGO_ENABLED=0 GOOS=linux go build -o /mini-orchestrator

CMD ["/mini-orchestrator"]
