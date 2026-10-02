build:
  cargo build

prod:
  cargo run --release -- --mqtt-addr=192.168.1.17 --mqtt-base-topic home/daikin

docker:
  docker build -t dirkvdb/daikin2mqtt:latest -f docker/BuildDockerfile .

dockerup:
  docker push dirkvdb/daikin2mqtt:latest
