# Maintainer: Kunobi Ninja <feedback@kunobi.ninja>
# Builds kache from the latest `main`. pkgver() derives the version from git,
# so this tracks unreleased/dev code — rebuild to update.
pkgname=kache-git
pkgver=0.0.0.r0.g0000000
pkgrel=1
pkgdesc='Content-addressed zero-copy build cache for Rust, C/C++ and more (latest git main)'
arch=('x86_64' 'aarch64')
url='https://github.com/kunobi-ninja/kache'
license=('Apache-2.0')
depends=('gcc-libs')
makedepends=('git' 'cargo')
provides=('kache')
conflicts=('kache')
options=('!lto')
source=("kache::git+https://github.com/kunobi-ninja/kache.git")
sha256sums=('SKIP')

pkgver() {
  cd "$srcdir/kache"
  printf '%s.r%s.g%s' \
    "$(git describe --tags --abbrev=0 2>/dev/null | sed 's/^v//' || echo 0.0.0)" \
    "$(git rev-list --count HEAD)" \
    "$(git rev-parse --short=7 HEAD)"
}

prepare() {
  cd "$srcdir/kache"
  cargo fetch --locked --target "$(rustc -vV | sed -n 's/host: //p')"
}

build() {
  cd "$srcdir/kache"
  CARGO_TARGET_DIR=target cargo build --frozen --release -p kache
}

package() {
  cd "$srcdir/kache"
  install -Dm0755 target/release/kache "$pkgdir/usr/bin/kache"

  local shell
  for shell in bash zsh fish elvish; do
    ./target/release/kache completions "$shell" > "kache.$shell"
  done
  install -Dm0644 kache.bash   "$pkgdir/usr/share/bash-completion/completions/kache"
  install -Dm0644 kache.zsh    "$pkgdir/usr/share/zsh/site-functions/_kache"
  install -Dm0644 kache.fish   "$pkgdir/usr/share/fish/vendor_completions.d/kache.fish"
  install -Dm0644 kache.elvish "$pkgdir/usr/share/elvish/lib/kache.elv"

  install -Dm0644 LICENSE "$pkgdir/usr/share/licenses/$pkgname/LICENSE"
}
