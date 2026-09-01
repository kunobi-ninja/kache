# Docker Bake configuration for kache service
# Build:     docker buildx bake -f packaging/docker-bake.hcl
# Dry run:   docker buildx bake -f packaging/docker-bake.hcl --print
# Push (CI): docker buildx bake -f packaging/docker-bake.hcl release

variable "REGISTRY" {
  default = "zondax"
}

variable "IMAGE_TAG" {
  default = "dev"
}

variable "VERSION" {
  default = "0.0.0"
}

variable "BUILD_VERSION" {
  default = "dev"
}

variable "BUILD_COMMIT" {
  default = "unknown"
}

variable "BUILD_DATE" {
  default = "unknown"
}

variable "PLATFORM" {
  default = "linux/amd64"
}

# A prerelease version carries a `-` (0.15.0-rc.1); a stable one never does.
# Compared against itself with the suffix stripped rather than pattern-matched,
# so it holds for -rc.N, -alpha.N, -beta.N and anything else added later.
function "is_prerelease" {
  params = [version]
  result = notequal(version, split("-", version)[0])
}

# `latest`, `vX` and `vX.Y` are FLOATING: whoever tracks them gets whatever was
# pushed last, without asking. A release candidate must never move them —
# otherwise tagging v0.15.0-rc.1 hands the experiment to everyone on
# zondax/kache:latest, which includes clusters running pullPolicy: Always, and
# the rc silently becomes everyone's kache on the next restart. That is the
# opposite of what a prerelease is for.
#
# The exact tags (0.15.0-rc.1, v0.15.0-rc.1) are always pushed: an rc has to be
# installable, just never by accident.
#
# Non-tag main builds keep moving `latest` as before — VERSION is 0.0.0 there,
# which is not a prerelease. This narrows prereleases only.
function "tags" {
  params = [name]
  result = compact([
    is_prerelease(VERSION) ? "" : "${REGISTRY}/${name}:latest",
    "${REGISTRY}/${name}:${IMAGE_TAG}",
    notequal(VERSION, "0.0.0") ? "${REGISTRY}/${name}:v${VERSION}" : "",
    notequal(VERSION, "0.0.0") && !is_prerelease(VERSION) ? "${REGISTRY}/${name}:v${split(".", VERSION)[0]}.${split(".", VERSION)[1]}" : "",
    notequal(VERSION, "0.0.0") && !is_prerelease(VERSION) ? "${REGISTRY}/${name}:v${split(".", VERSION)[0]}" : "",
  ])
}

group "default" {
  targets = ["service"]
}

group "release" {
  targets = ["service-release"]
}

target "service" {
  dockerfile = "packaging/docker/service.Dockerfile"
  context    = "."
  platforms  = [PLATFORM]
  tags       = tags("kache")
  args = {
    BUILD_VERSION = BUILD_VERSION
    BUILD_COMMIT  = BUILD_COMMIT
    BUILD_DATE    = BUILD_DATE
  }
  output = ["type=docker"]
}

target "service-release" {
  inherits  = ["service"]
  platforms = ["linux/amd64"]
  output    = ["type=registry"]
}
