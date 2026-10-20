FROM golang:1.23-bookworm AS build
WORKDIR /src
COPY bench/go.mod bench/go.sum ./
RUN go mod download
COPY bench/*.go ./
RUN CGO_ENABLED=0 go build -trimpath -o /bench .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /bench /bench
ENTRYPOINT ["/bench"]
