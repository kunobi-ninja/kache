{
  description = "kache - Zero-copy, content-addressed build cache for Rust, C/C++ and more";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    treefmt-nix = {
      url = "github:numtide/treefmt-nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      rust-overlay,
      treefmt-nix,
    }:
    let
      inherit (nixpkgs) lib;

      # x86_64-darwin is deliberately absent: nixpkgs 26.11 dropped the
      # platform, so `import nixpkgs { system = "x86_64-darwin"; }` throws
      # (#597). Intel macOS is still a supported *release* target — install the
      # tarball or `cargo install` there.
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "aarch64-darwin"
      ];

      forAllSystems =
        f:
        lib.genAttrs systems (
          system:
          f (
            import nixpkgs {
              inherit system;
              overlays = [ self.overlays.default ];
            }
          )
        );

      kacheOverlay =
        final: _prev:
        let
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

          # Exposed so devShells (and downstream consumers) get the toolchain
          # pinned in rust-toolchain.toml instead of nixpkgs' rustc, which can
          # lag the crate's rust-version.
          kache-rust-toolchain = rustToolchain;
        };

      # Only the Nix sources, so editing Rust code doesn't invalidate the
      # formatting check.
      nixSources = lib.fileset.toSource {
        root = ./.;
        fileset = lib.fileset.unions [
          ./flake.nix
          ./nix
        ];
      };

      # One config drives both `nix fmt` and the formatting check, so the two
      # cannot drift apart.
      treefmtFor =
        pkgs:
        treefmt-nix.lib.evalModule pkgs {
          projectRootFile = "flake.nix";
          programs.nixfmt.enable = true;
        };

      # Wraps ./nix/module.nix so the exported module works on its own. Without
      # this, the option default falls back to nixpkgs' rustPlatform, whose
      # rustc is below the crate's rust-version, and cargo refuses to build it.
      # `pkgs.kache` comes first so a consumer who applied the overlay keeps
      # their own nixpkgs; mkDefault so an explicit `services.kache.package`
      # still wins.
      moduleFor =
        module:
        {
          pkgs,
          lib,
          ...
        }:
        {
          imports = [ module ];
          services.kache.package = lib.mkDefault (
            pkgs.kache or self.packages.${pkgs.stdenv.hostPlatform.system}.kache
          );
        };
    in
    {
      nixosModules = {
        kache = moduleFor ./nix/module.nix;
        default = self.nixosModules.kache;
      };

      # The same module works for nix-darwin (launchd vs systemd is handled internally).
      darwinModules = {
        kache = moduleFor ./nix/module.nix;
        default = self.darwinModules.kache;
      };

      overlays = {
        # NOTE: this one is NOT self-sufficient. It reads `final.rust-bin`, so
        # it only works when rust-overlay is applied first; on its own it fails
        # with "attribute 'rust-bin' missing" as soon as the derivation is
        # forced. Use `overlays.default` unless you already apply rust-overlay.
        kache = kacheOverlay;
        default = lib.composeManyExtensions [
          rust-overlay.overlays.default
          kacheOverlay
        ];
      };

      packages = forAllSystems (pkgs: {
        kache = pkgs.kache;
        default = pkgs.kache;
      });

      # mise stays the primary tool manager for this repo (see mise.toml); this
      # shell covers the Nix path with the pinned toolchain plus the tools
      # `just check` and `just audit` shell out to. RUSTC_WRAPPER is left alone
      # so dogfooding kache on kache keeps working inside the shell.
      devShells = forAllSystems (pkgs: {
        default = pkgs.mkShell {
          packages = [
            pkgs.kache-rust-toolchain
            pkgs.just
            pkgs.cargo-deny
            (treefmtFor pkgs).config.build.wrapper
          ];
        };
      });

      formatter = forAllSystems (pkgs: (treefmtFor pkgs).config.build.wrapper);

      checks = forAllSystems (
        pkgs:
        {
          package = pkgs.kache;

          # Same treefmt config as `nix fmt`, scoped to nixSources so a Rust
          # edit doesn't invalidate it.
          formatting = (treefmtFor pkgs).config.build.check nixSources;
        }
        // lib.optionalAttrs pkgs.stdenv.hostPlatform.isLinux {
          # Evaluates the NixOS module end to end, which is what catches option
          # type and rename regressions. `nix flake check --no-build` stops at
          # evaluation, so the interesting part runs even without building.
          nixos-module =
            let
              machine = lib.nixosSystem {
                modules = [
                  self.nixosModules.kache
                  {
                    nixpkgs.pkgs = pkgs;
                    boot.loader.grub.enable = false;
                    fileSystems."/" = {
                      device = "/dev/null";
                      fsType = "ext4";
                    };
                    system.stateVersion = lib.trivial.release;

                    services.kache = {
                      enable = true;
                      daemon.enable = true;
                      settings.cache = {
                        local_max_size = "10GB";
                        remote = {
                          type = "s3";
                          bucket = "kache-check";
                        };
                      };
                    };
                  }
                ];
              };
              inherit (machine.config.systemd.user.services.kache) serviceConfig;
            in
            pkgs.runCommand "kache-check-nixos-module" { } ''
              mkdir -p "$out"

              cp ${machine.config.environment.etc."kache/config.toml".source} "$out/config.toml"
              grep -q 'local_max_size = "10GB"' "$out/config.toml"
              grep -q 'bucket = "kache-check"' "$out/config.toml"

              printf '%s\n' ${lib.escapeShellArg serviceConfig.ExecStart} > "$out/exec-start"
              grep -q 'daemon run' "$out/exec-start"

              # The module must resolve to the overlay's package, which is built
              # with the rust-toolchain.toml toolchain rather than nixpkgs' rustc.
              grep -qF '${pkgs.kache}' "$out/exec-start"
            '';
        }
      );
    };
}
