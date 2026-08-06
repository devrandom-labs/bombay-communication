{
  description = "fastpass — two-lane priority merge (bombay card #225)";
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    utils.url = "github:numtide/flake-utils";
    fenix = {
      url = "github:nix-community/fenix";
      inputs = {
        nixpkgs.follows = "nixpkgs";
      };
    };
  };
  outputs =
    {
      self,
      nixpkgs,
      utils,
      fenix,
      ...
    }:
    utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = nixpkgs.legacyPackages.${system};

        # STABLE, pinned toolchain — read from rust-toolchain.toml so Nix and
        # plain rustup resolve the SAME toolchain (bombay card #60 pattern).
        # The sha256 covers the channel manifest (system-independent).
        toolchain = fenix.packages.${system}.fromToolchainFile {
          file = ./rust-toolchain.toml;
          sha256 = "sha256-mvUGEOHYJpn3ikC5hckneuGixaC+yGrkMM/liDIDgoU=";
        };

        # Nightly toolchain for the on-demand MIRI lane (bombay card #150
        # pattern). NOT a build toolchain, kept out of the default shell.
        # MIRI is the only tool that can see this crate's concurrency: loom
        # requires code under test to opt in (`cfg(loom)`), and flume — which
        # owns the channel internals — ships no such instrumentation. MIRI is
        # an interpreter, so it executes flume's real std::sync atomics.
        miriToolchain =
          (fenix.packages.${system}.toolchainOf {
            channel = "nightly";
            date = "2026-06-15";
            sha256 = "sha256-oXipquOa/9M0uuo8wGuRaY2+ZqLGywZOOnRK05Mm0a0=";
          }).withComponents
            [
              "cargo"
              "rustc"
              "rust-src"
              "rust-std"
              "miri"
            ];
      in
      {
        devShells.default = pkgs.mkShell {
          packages = [
            toolchain
            pkgs.cargo-edit
            pkgs.cargo-expand
            pkgs.cargo-nextest
          ];
        };

        # `nix develop .#miri` — the MIRI lane's toolchain, on demand.
        devShells.miri = pkgs.mkShell {
          packages = [ miriToolchain ];
          shellHook = ''
            echo "fastpass MIRI shell — nightly, on-demand only."
            echo "  cargo miri setup"
            echo "  MIRIFLAGS=\"-Zmiri-many-seeds=..8\" cargo miri test -p fastpass"
          '';
        };
      }
    );
}
