{
  lib,
  rustPlatform,
  fetchurl,
  apple-sdk_15,
  cacert,
  stdenv,
}:
let
  cargoToml = builtins.fromTOML (builtins.readFile ../Cargo.toml);

  fetchurlWithCratesUserAgent = args:
    fetchurl (args
      // {
        curlOptsList = (args.curlOptsList or []) ++ ["-A" "kache-nix"];
      });

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
    root = ../.;
    fileset = lib.fileset.unions [
      ../Cargo.toml
      ../Cargo.lock
      ../crates
      ../src
    ];
  };

  cargoLock = {
    lockFile = ../Cargo.lock;
    outputHashes = {
      "kunobi-auth-0.2.0" = "sha256-5qwhst8gt6KY9A37j0loEHBICzIAaVuyvtdOjTjRbdk=";
      "kunobi-ha-0.5.0" = "sha256-y1Kye3/WfnnkcuThOr8AzvlQIkvKVMCOrT5cOohMKE4=";
    };
  };

  cargoBuildFlags = ["-p" "kache"];
  cargoTestFlags = ["-p" "kache"];

  buildInputs = lib.optionals stdenv.hostPlatform.isDarwin [
    apple-sdk_15
  ];

  # The tmutil xattr test shells out to /usr/bin/tmutil which isn't in the sandbox.
  checkFlags = lib.optionals stdenv.hostPlatform.isDarwin [
    "--skip=store::tests::test_exclude_from_indexing_sets_tmutil_xattr"
  ];

  # Avoid bootstrapping loop: don't let kache wrap itself during build
  env.RUSTC_WRAPPER = "";

  # reqwest (rustls) loads system CA certs when building a client, even for the
  # plain-HTTP localhost planner tests. The sandbox has no trust store, so point
  # it at the cacert bundle to keep client construction from failing.
  env.SSL_CERT_FILE = "${cacert}/etc/ssl/certs/ca-bundle.crt";

  meta = {
    description = "Zero-copy, content-addressed Rust build cache";
    homepage = "https://github.com/kunobi-ninja/kache";
    license = lib.licenses.asl20;
    mainProgram = "kache";
  };
}
