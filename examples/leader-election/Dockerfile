# Build context is the repository root, since this example's go.mod uses a
# replace directive to depend on the parent natscontext module.
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY . .
WORKDIR /src/examples/leader-election
RUN go build -o /out/leader-election .

FROM alpine:3.20
COPY --from=build /out/leader-election /usr/local/bin/leader-election
ENTRYPOINT ["leader-election"]
