{
  description = "kache - Zero-copy, content-addressed Rust build cache";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = {
    self,
    nixpkgs,
    flake-utils,
  }:
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

      overlays.default = _final: prev: {
        kache = prev.callPackage ./nix/package.nix {};
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
