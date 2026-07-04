# One Dockerfile for every service; pick which via --build-arg SERVICE=order|inventory.
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG SERVICE
RUN CGO_ENABLED=0 go build -o /app ./services/${SERVICE}

FROM gcr.io/distroless/static
COPY --from=build /app /app
ENTRYPOINT ["/app"]
