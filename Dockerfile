FROM --platform=$BUILDPLATFORM golang:1.20 AS build

ARG TARGETOS=linux
ARG TARGETARCH=amd64

ENV GOPROXY=https://goproxy.cn,direct
ENV GO111MODULE=on

WORKDIR /go/cache
COPY go.mod .
COPY go.sum .
RUN go mod download

WORKDIR /go/release
COPY . .

# Keep the image compatible with the original TangSengDaoDao compose:
#   command: "api"
# The final stage ENTRYPOINT is /home/app, so Docker passes "api" as argv[1].
# The git metadata commands are defensive because forks may have no tags.
RUN GIT_COMMIT=$(git rev-parse HEAD 2>/dev/null || echo "unknown") && \
    GIT_COMMIT_DATE=$(git log --date=iso8601-strict -1 --pretty=%ct 2>/dev/null || date +%s) && \
    GIT_VERSION=$(git describe --tags --abbrev=0 2>/dev/null || echo "v0.0.0") && \
    GIT_TREE_STATE=$(test -n "$(git status --porcelain 2>/dev/null)" && echo "dirty" || echo "clean") && \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build \
      -ldflags="-w -extldflags '-static' -X main.Commit=$GIT_COMMIT -X main.CommitDate=$GIT_COMMIT_DATE -X main.Version=$GIT_VERSION -X main.TreeState=$GIT_TREE_STATE" \
      -installsuffix cgo -o app ./main.go

FROM alpine AS prod
COPY --from=build /etc/passwd /etc/passwd
COPY --from=build /usr/share/zoneinfo/Asia/Shanghai /etc/localtime
RUN mkdir -p /usr/share/zoneinfo/Asia && \
    ln -s /etc/localtime /usr/share/zoneinfo/Asia/Shanghai
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
WORKDIR /home
COPY --from=build /go/release/app /home/app
COPY --from=build /go/release/assets /home/assets
COPY --from=build /go/release/configs /home/configs
RUN echo "Asia/Shanghai" > /etc/timezone
ENV TZ=Asia/Shanghai

ENTRYPOINT ["/home/app"]
