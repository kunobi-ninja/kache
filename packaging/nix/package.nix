{
  lib,
  rustPlatform,
  cacert,
  stdenv,
  bashInteractive,
  zsh,
}:
let
  cargoToml = builtins.fromTOML (builtins.readFile ../../Cargo.toml);
in
rustPlatform.buildRustPackage {
  pname = "kache";
  version = cargoToml.package.version;

  src = lib.fileset.toSource {
    root = ../../.;
    fileset = lib.fileset.unions [
      ../../Cargo.toml
      ../../Cargo.lock
      ../../assets
      ../../build.rs
      ../../crates
      # The report schema the unit tests validate reports against.
      ../../docs/commands/report.schema.json
      ../../launcher
      ../../src
      ../../tests/fixtures
    ];
  };

  cargoLock = {
    lockFile = ../../Cargo.lock;
  };

  cargoBuildFlags = [
    "-p"
    "kache"
  ];
  cargoTestFlags = [
    "-p"
    "kache"
  ];

  nativeCheckInputs = [
    bashInteractive
    zsh
  ];

  # The tmutil xattr test shells out to /usr/bin/tmutil which isn't in the sandbox.
  checkFlags = lib.optionals stdenv.hostPlatform.isDarwin [
    "--skip=store::tests::test_exclude_from_indexing_sets_tmutil_xattr"
    # Nix's sandbox rejects sandbox_apply for these nested sandbox fixtures.
    # The regular macOS CI job runs both against real allowed/denied processes.
    "--skip=fallback::macos::tests::policy_distinguishes_denied_and_allowed_output"
    "--skip=sandbox_preflight_bypasses_denied_server_but_keeps_allowed_server"
    # Compiler handoff requires an unrestricted client. These fixtures run
    # in the regular macOS job; Nix's Seatbelt policy requires local execution.
    "--skip=daemon::execution::tests::disconnected_accepted_compiler_is_cancelled_and_reaped"
    "--skip=daemon::execution::tests::draining_before_commit_releases_worker_without_starting_a_compiler"
    "--skip=daemon::execution::tests::execution_handoff_streams_two_compilers_without_serializing_or_mixing_clients"
    "--skip=daemon::execution::tests::execution_jobserver_descriptors_reach_only_the_compiler"
    "--skip=daemon::execution::tests::execution_keeps_a_silent_compiler_alive_past_the_daemon_idle_timeout"
    "--skip=daemon::execution::tests::only_accepted_execution_marks_build_activity"
    "--skip=daemon::execution::tests::original_compiler_shim_uses_the_clients_relative_path"
    "--skip=daemon::execution::tests::prepared_compiler_never_runs_without_commit_and_releases_its_resources"
    "--skip=daemon::execution::tests::execution_rejects_sandboxed_client_using_kernel_identity"
  ];

  # planner_client / remote_backend tests bind 127.0.0.1. Darwin's Nix sandbox
  # denies that unless this is set.
  __darwinAllowLocalNetworking = true;

  # The suite runs ~2000 tests at full parallelism; nix-daemon's default soft
  # descriptor limit (often 1024) is low enough for the parallel run to hit
  # EMFILE, which surfaced as spurious single-test failures in flake builds
  # (#756). Raise the soft limit toward the hard limit; best-effort so a
  # builder with a lower hard cap still runs.
  preCheck = ''
    # Completion activation needs programmable completion, which stdenv's
    # minimal Bash omits. Keep both supported shell fixtures available.
    export PATH="${
      lib.makeBinPath [
        bashInteractive
        zsh
      ]
    }:$PATH"
    bash -c 'type complete >/dev/null'
    ulimit -n 4096 2>/dev/null || true
  '';

  # Shell fixtures use the build shell and its builtins.
  env.KACHE_TEST_SHELL = stdenv.shell;

  # Avoid bootstrapping loop: don't let kache wrap itself during build
  env.RUSTC_WRAPPER = "";

  postInstall = lib.optionalString stdenv.hostPlatform.isUnix ''
    mkdir -p $out/lib/kache
    for name in cc c++ gcc g++ clang clang++ cargo rustdoc; do
      ln -s $out/bin/kache $out/lib/kache/$name
    done
    # Marks the farm so another kache on PATH skips it (see
    # compiler::shim::SHIM_DIR_MARKER).
    touch $out/lib/kache/.kache-shims
    ln -s lib/kache $out/shims
  '';

  # reqwest (rustls) loads system CA certs when building a client, even for the
  # plain-HTTP localhost planner tests. The sandbox has no trust store, so point
  # it at the cacert bundle to keep client construction from failing.
  env.SSL_CERT_FILE = "${cacert}/etc/ssl/certs/ca-bundle.crt";

  meta = {
    description = "Zero-copy, content-addressed build cache for Rust, C/C++ and more";
    homepage = "https://github.com/kunobi-ninja/kache";
    license = lib.licenses.asl20;
    mainProgram = "kache";
  };
}
