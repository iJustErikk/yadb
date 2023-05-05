# no idea how to cache this across codespace instances :)
FROM rust:1.67

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    clang \
    librocksdb-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app

COPY Cargo.toml .

# trick to cache dependencies
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build
RUN rm -r src
COPY src src

RUN cargo build
CMD ["cargo", "run"]
