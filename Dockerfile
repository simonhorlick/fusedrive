#
# docker build -t fusedrive .
#
# docker run -it --device /dev/fuse --cap-add SYS_ADMIN -v /media/drive:/media/drive:shared -v /home/core/fusedrive:/var/fusedrive fusedrive
#
#
FROM golang:1.11-stretch as builder

MAINTAINER Simon Horlick <simonhorlick@gmail.com>

WORKDIR $GOPATH/src/github.com/simonhorlick/fusedrive

# Grab and install the latest version of of btcd and all related dependencies.
RUN git clone https://github.com/simonhorlick/fusedrive.git . \
    &&  GO111MODULE=on go install -v .

# Start a new image
FROM debian:stretch as final

# Install runtime dependencies.
RUN apt update && apt install -y fuse

# Copy the compiled binaries from the builder image.
COPY --from=builder /go/bin/fusedrive /bin/

COPY entrypoint.sh /

CMD [ "/entrypoint.sh" ]
