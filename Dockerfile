# This Dockerfile uses multistage build - makes it easy to read and maintain
# (plus keeps the build efficient and the image small!)

# this stage builds our service
# we use golang:1.14-alpine for the Go compiler, our dependencies and various
# system libraries
# we don't need these after we compile our binary file (from the Go files)
FROM golang:1.14-alpine AS build
WORKDIR /go/src/proglog
COPY . .
# Cgo is disabled so that we statically compile the binaries
# this allows them to run in the scratch image (below) which doesn't contain
# the system libraries needed to run dynamically
RUN CGO_ENABLED=0 go build -o /go/bin/proglog ./cmd/proglog
# install grpc_health_probe executable in image
RUN GRPC_HEALTH_PROBE_VERSION=v0.3.2 && \
wget -qO/go/bin/grpc_health_probe \
https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/\
${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64 && \
chmod +x /go/bin/grpc_health_probe

# this stage runs our service
# the scratch empty image is the smallest Docker image!
#
# using the scratch image helps with thinking of containers as being immutable
# instead of exec-ing into a container and mutating the image by installing
# tools or changing the filesystem, we simply run a short-lived container that
# has the tool you need :)
FROM scratch
# copy our binary into this scratch image, and this is the image we deploy!
COPY --from=build /go/bin/proglog /bin/proglog
copy --from=build /go/bin/grpc_health_probe /bin/grpc_health_probe
ENTRYPOINT ["/bin/proglog"]