# NixOS and nix-darwin module for kache.
# Works on both platforms - uses launchd on macOS, systemd on Linux.
{
  config,
  lib,
  options,
  pkgs,
  ...
}: let
  cfg = config.services.kache;
  tomlFormat = pkgs.formats.toml {};

  # Settings maps directly to config.toml. Freeform type allows arbitrary
  # keys so the module doesn't break when kache adds new config options.
  # Recursively strip null values - TOML has no null representation,
  # and the typed options use null as "use kache's built-in default".
  stripNulls = lib.filterAttrsRecursive (_: v: v != null);

  configFile = tomlFormat.generate "kache-config" (stripNulls cfg.settings);

  kacheExe = lib.getExe cfg.package;

  # Check which platform options exist rather than querying pkgs,
  # which would create a circular dependency with lib.optionals.
  hasLaunchd = options ? launchd;
  hasSystemd = options ? systemd;
in {
  options.services.kache = {
    enable = lib.mkEnableOption "kache, a Rust build cache";

    package = lib.mkPackageOption pkgs "kache" {};

    rustcWrapper = lib.mkOption {
      type = lib.types.bool;
      default = true;
      description = ''
        Set `RUSTC_WRAPPER` system-wide so all Rust builds use the cache.
      '';
    };

    daemon = {
      enable = lib.mkEnableOption "the kache background daemon";

      logLevel = lib.mkOption {
        type = lib.types.str;
        default = "kache=info";
        description = "Log level for the daemon (`KACHE_LOG` value).";
      };
    };

    settings = lib.mkOption {
      description = ''
        Configuration written to `config.toml`.

        The typed options below cover known fields for documentation and
        validation. Any additional keys are passed through as-is, so the
        module stays usable when kache adds new config options.

        See <https://github.com/kunobi-ninja/kache#configuration> for
        the full reference.
      '';
      default = {};
      type = lib.types.submodule {
        freeformType = tomlFormat.type;

        options.cache = lib.mkOption {
          description = "Cache settings.";
          default = {};
          type = lib.types.submodule {
            freeformType = tomlFormat.type;

            options = {
              local_store = lib.mkOption {
                type = lib.types.nullOr lib.types.str;
                default = null;
                description = "Local cache store directory. Default: `~/.cache/kache`.";
                example = "~/.cache/kache";
              };

              local_max_size = lib.mkOption {
                type = lib.types.nullOr lib.types.str;
                default = null;
                description = "Maximum local store size. Default: `50GB`.";
                example = "50GB";
              };

              cache_executables = lib.mkOption {
                type = lib.types.nullOr lib.types.bool;
                default = null;
                description = "Whether to cache bin/dylib/cdylib outputs.";
              };

              clean_incremental = lib.mkOption {
                type = lib.types.nullOr lib.types.bool;
                default = null;
                description = "Auto-clean incremental compilation dirs during GC.";
              };

              compression_level = lib.mkOption {
                type = lib.types.nullOr (lib.types.ints.between 1 22);
                default = null;
                description = "Zstd compression level (1-22). Default: 3.";
              };

              s3_concurrency = lib.mkOption {
                type = lib.types.nullOr lib.types.ints.positive;
                default = null;
                description = "Max concurrent S3 operations. Default: 16.";
              };

              daemon_idle_timeout_secs = lib.mkOption {
                type = lib.types.nullOr lib.types.ints.unsigned;
                default = null;
                description = "Daemon idle timeout in seconds. 0 = no timeout. Default: 3600.";
              };

              remote = lib.mkOption {
                description = "S3 remote cache settings.";
                default = {};
                type = lib.types.submodule {
                  freeformType = tomlFormat.type;

                  options = {
                    type = lib.mkOption {
                      type = lib.types.nullOr lib.types.str;
                      default = null;
                      description = "Remote type. Currently only `s3`.";
                    };

                    bucket = lib.mkOption {
                      type = lib.types.nullOr lib.types.str;
                      default = null;
                      description = "S3 bucket for remote cache.";
                    };

                    endpoint = lib.mkOption {
                      type = lib.types.nullOr lib.types.str;
                      default = null;
                      description = "Custom S3 endpoint (for Ceph/MinIO/R2).";
                    };

                    region = lib.mkOption {
                      type = lib.types.nullOr lib.types.str;
                      default = null;
                      description = "AWS region. Default: `us-east-1`.";
                    };

                    prefix = lib.mkOption {
                      type = lib.types.nullOr lib.types.str;
                      default = null;
                      description = "S3 key prefix for artifacts. Default: `artifacts`.";
                    };

                    profile = lib.mkOption {
                      type = lib.types.nullOr lib.types.str;
                      default = null;
                      description = "AWS profile name for credential lookup.";
                    };
                  };
                };
              };
            };
          };
        };
      };
    };
  };

  config = lib.mkIf cfg.enable (lib.mkMerge ([
    # Install the package and config
    {
      environment.systemPackages = [cfg.package];
      environment.etc."kache/config.toml".source = configFile;
    }

    # Set RUSTC_WRAPPER system-wide
    (lib.mkIf cfg.rustcWrapper {
      environment.variables.RUSTC_WRAPPER = kacheExe;
    })
  ]
  # macOS: launchd user agent
  ++ lib.optionals hasLaunchd [
    (lib.mkIf cfg.daemon.enable {
      launchd.user.agents.kache = {
        serviceConfig = {
          Label = "com.zondax.kache";
          ProgramArguments = [kacheExe "daemon" "run"];
          RunAtLoad = true;
          KeepAlive = {
            SuccessfulExit = false;
          };
          EnvironmentVariables = {
            KACHE_LOG = cfg.daemon.logLevel;
            KACHE_CONFIG = configFile;
          };
          ThrottleInterval = 5;
        };
      };
    })
  ]
  # Linux: systemd user service
  ++ lib.optionals hasSystemd [
    (lib.mkIf cfg.daemon.enable {
      systemd.user.services.kache = {
        description = "kache build cache daemon";
        after = ["default.target"];
        wantedBy = ["default.target"];
        serviceConfig = {
          Type = "simple";
          ExecStart = "${kacheExe} daemon run";
          Restart = "on-failure";
          RestartSec = "5s";
        };
        environment = {
          KACHE_LOG = cfg.daemon.logLevel;
          KACHE_CONFIG = configFile;
        };
      };
    })
  ]));
}
