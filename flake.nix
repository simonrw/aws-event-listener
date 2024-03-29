{
  inputs = {
    flake-utils.url = "github:numtide/flake-utils";
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    crane.url = "github:ipetkov/crane";
    crane.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs = { self, flake-utils, crane, nixpkgs }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        craneLib = crane.lib.${system};

        pkgs = (import nixpkgs) {
          inherit system;
        };

        commonArgs = {
          src = craneLib.cleanCargoSource ./.;
          nativeBuildInputs = [
            pkgs.rustc
            pkgs.cargo
            pkgs.libiconv
          ] ++ pkgs.lib.optionals pkgs.stdenv.isDarwin [
            pkgs.darwin.apple_sdk.frameworks.Cocoa
          ];
        };

        cargoArtifacts = craneLib.buildDepsOnly (commonArgs // {
          pname = "aws-event-listener-deps";
        });

        clippy = craneLib.cargoClippy (commonArgs // {
          inherit cargoArtifacts;
          cargoClippyExtraArgs = "--all-targets -- --deny warnings";
        });

        aws-event-listener = craneLib.buildPackage (commonArgs // {
          inherit cargoArtifacts;
        });

        coverage = craneLib.cargoTarpaulin (commonArgs // {
          inherit cargoArtifacts;
          # cargoTarpaulinExtraArgs = "--features integration --skip-clean --out Xml --output-dir $out";
        });
      in
      rec {
        packages.default = aws-event-listener;
        checks = {
          inherit aws-event-listener clippy;
        };

        devShells.default = pkgs.mkShell {
          nativeBuildInputs = packages.default.nativeBuildInputs;
          RUST_SRC_PATH = "${pkgs.rustPlatform.rustLibSrc}";
        };
      }
    );
}
