FROM golang:1.26.10 AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG CMD
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/bin ./cmd/${CMD}

FROM gcr.io/distroless/static-debian13
WORKDIR /
COPY --from=build /app/bin /app
USER nonroot:nonroot
ENTRYPOINT ["/app"]