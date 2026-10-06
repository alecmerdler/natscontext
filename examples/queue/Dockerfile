# Build context is the repository root, since this example's go.mod uses a
# replace directive to depend on the parent natscontext module.
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY . .
WORKDIR /src/examples/queue
RUN go build -o /out/queue .

FROM alpine:3.20
COPY --from=build /out/queue /usr/local/bin/queue
ENTRYPOINT ["queue"]
