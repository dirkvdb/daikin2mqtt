{
  description = "Daikin MQTT bridge and minimal container image";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs = { self, nixpkgs }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" ];
      forAllSystems = nixpkgs.lib.genAttrs systems;
    in
    {
      packages = forAllSystems (system:
        let
          pkgs = import nixpkgs { inherit system; };
          manifest = fromTOML (builtins.readFile ./Cargo.toml);
          src = pkgs.lib.fileset.toSource {
            root = ./.;
            fileset = pkgs.lib.fileset.unions [
              ./Cargo.toml
              ./Cargo.lock
              ./LICENSE
              ./src
              ./resources
              ./secretspec.toml
            ];
          };
          app = pkgs.pkgsStatic.rustPlatform.buildRustPackage {
            pname = manifest.package.name;
            version = manifest.package.version;
            inherit src;
            cargoLock.lockFile = ./Cargo.lock;
            nativeBuildInputs = [ pkgs.cmake ];
            dontUseCmakeConfigure = true;
            preCheck = ''
              export SSL_CERT_FILE=${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt
            '';

            meta = {
              description = "Interface between Daikin heat pumps and MQTT";
              license = pkgs.lib.licenses.mit;
              mainProgram = "daikin2mqtt";
              platforms = systems;
            };
          };
        in
        {
          default = app;
          daikin2mqtt = app;
          container = pkgs.dockerTools.buildLayeredImage {
            name = "dirkvdb/daikin2mqtt";
            tag = "latest";
            contents = [ app pkgs.cacert ];
            extraCommands = ''
              mkdir -p data tmp
              chmod 1777 tmp
            '';
            config = {
              Entrypoint = [ "${app}/bin/daikin2mqtt" ];
              WorkingDir = "/";
              Env = [
                "SSL_CERT_FILE=${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt"
                "SECRETSPEC_PROVIDER=env"
                "DAIKIN_DATA_DIR=/data"
                "HOME=/data"
              ];
              ExposedPorts."8076/tcp" = { };
            };
          };
        });

      checks = forAllSystems (system: {
        inherit (self.packages.${system}) daikin2mqtt container;
      });
    };
}
