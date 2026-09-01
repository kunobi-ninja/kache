{
  lib,
  rustPlatform,
  fetchurl,
  cacert,
  stdenv,
}:
let
  cargoToml = builtins.fromTOML (builtins.readFile ../../Cargo.toml);

  fetchurlWithCratesUserAgent =
    args:
    fetchurl (
      args
      // {
        curlOptsList = (args.curlOptsList or [ ]) ++ [
          "-A"
          "kache-nix"
        ];
      }
    );

  buildRustPackage = rustPlatform.buildRustPackage.override {
    importCargoLock = rustPlatform.importCargoLock.override {
      fetchurl = fetchurlWithCratesUserAgent;
    };
  };
in
buildRustPackage {
  pname = "kache";
  version = cargoToml.package.version;

  src = lib.fileset.toSource {
    root = ../../.;
    fileset = lib.fileset.unions [
      ../../Cargo.toml
      ../../Cargo.lock
      ../../assets
      ../../crates
      ../../src
      ../../tests/fixtures
    ];
  };

  cargoLock = {
    lockFile = ../../Cargo.lock;
    outputHashes = {
      "kunobi-auth-0.2.0" = "sha256-5qwhst8gt6KY9A37j0loEHBICzIAaVuyvtdOjTjRbdk=";
      "kunobi-ha-0.5.0" = "sha256-S7i/hlpqfWnyv/3n8dyD90sMuICM/C4ouFLk536oc5k=";
    };
  };

  cargoBuildFlags = [
    "-p"
    "kache"
  ];
  cargoTestFlags = [
    "-p"
    "kache"
  ];

  # The tmutil xattr test shells out to /usr/bin/tmutil which isn't in the sandbox.
  checkFlags = lib.optionals stdenv.hostPlatform.isDarwin [
    "--skip=store::tests::test_exclude_from_indexing_sets_tmutil_xattr"
  ];

  # The suite runs ~2000 tests at full parallelism; nix-daemon's default soft
  # descriptor limit (often 1024) is low enough for the parallel run to hit
  # EMFILE, which surfaced as spurious single-test failures in flake builds
  # (#756). Raise the soft limit toward the hard limit; best-effort so a
  # builder with a lower hard cap still runs.
  preCheck = ''
    ulimit -n 4096 2>/dev/null || true
  '';

  # Avoid bootstrapping loop: don't let kache wrap itself during build
  env.RUSTC_WRAPPER = "";

  postInstall = lib.optionalString stdenv.hostPlatform.isUnix ''
    mkdir -p $out/lib/kache
    for name in cc c++ gcc g++ clang clang++; do
      ln -s $out/bin/kache $out/lib/kache/$name
    done
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
