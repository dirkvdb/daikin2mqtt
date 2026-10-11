build:
  cargo build

test:
  cargo nextest run

prod:
  cargo run --release -- --mqtt-addr=192.168.1.17 --mqtt-base-topic home/daikin

# Build and load the AMD64 Nix image without publishing it.
docker:
  nix build .#packages.x86_64-linux.container --out-link result-container
  docker load --input result-container

dockerup: docker
  docker push dirkvdb/daikin2mqtt:latest
