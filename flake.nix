{
  description = "kache - Zero-copy, content-addressed Rust build cache";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = {
    self,
    nixpkgs,
    flake-utils,
    rust-overlay,
  }:
    let
      kacheOverlay = final: _prev: let
        rustToolchain = final.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;
        rustPlatform = final.makeRustPlatform {
          cargo = rustToolchain;
          rustc = rustToolchain;
        };
      in
        {
          kache = final.callPackage ./nix/package.nix {
            inherit rustPlatform;
          };
        };
    in
    {
      nixosModules = {
        kache = import ./nix/module.nix;
        default = self.nixosModules.kache;
      };

      # The same module works for nix-darwin (launchd vs systemd is handled internally).
      darwinModules = {
        kache = import ./nix/module.nix;
        default = self.darwinModules.kache;
      };

      overlays = {
        kache = kacheOverlay;
        default = nixpkgs.lib.composeManyExtensions [
          rust-overlay.overlays.default
          kacheOverlay
        ];
      };
    }
    // flake-utils.lib.eachDefaultSystem (system: let
      pkgs = import nixpkgs {
        inherit system;
        overlays = [self.overlays.default];
      };
    in {
      packages = {
        kache = pkgs.kache;
        default = pkgs.kache;
      };
    });
}
