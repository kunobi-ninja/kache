{
  lib,
  rustPlatform,
  apple-sdk_15,
  stdenv,
}:
rustPlatform.buildRustPackage {
  pname = "kache";
  version = "0-unstable";

  src = lib.fileset.toSource {
    root = ../.;
    fileset = lib.fileset.unions [
      ../Cargo.toml
      ../Cargo.lock
      ../src
    ];
  };

  cargoHash = "sha256-sKGGDkVAt9vn8t5O0b419/REE89M5hrwyZ8erld7mcc=";

  buildInputs = lib.optionals stdenv.hostPlatform.isDarwin [
    apple-sdk_15
  ];

  # The tmutil xattr test shells out to /usr/bin/tmutil which isn't in the sandbox.
  checkFlags = lib.optionals stdenv.hostPlatform.isDarwin [
    "--skip=store::tests::test_exclude_from_indexing_sets_tmutil_xattr"
  ];

  # Avoid bootstrapping loop: don't let kache wrap itself during build
  env.RUSTC_WRAPPER = "";

  meta = {
    description = "Zero-copy, content-addressed Rust build cache";
    homepage = "https://github.com/kunobi-ninja/kache";
    license = lib.licenses.asl20;
    mainProgram = "kache";
  };
}
