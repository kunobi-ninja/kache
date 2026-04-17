# Docker Bake configuration for kache service
# Build:     docker buildx bake -f docker-bake.hcl
# Dry run:   docker buildx bake -f docker-bake.hcl --print
# Push:      docker buildx bake -f docker-bake.hcl release

variable "REGISTRY" {
  default = "ghcr.io/kunobi-ninja"
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

function "tags" {
  params = [name]
  result = compact([
    "${REGISTRY}/${name}:latest",
    "${REGISTRY}/${name}:${IMAGE_TAG}",
    notequal(VERSION, "0.0.0") ? "${REGISTRY}/${name}:v${VERSION}" : "",
    notequal(VERSION, "0.0.0") ? "${REGISTRY}/${name}:v${split(".", VERSION)[0]}.${split(".", VERSION)[1]}" : "",
    notequal(VERSION, "0.0.0") ? "${REGISTRY}/${name}:v${split(".", VERSION)[0]}" : "",
  ])
}

group "default" {
  targets = ["service"]
}

group "release" {
  targets = ["service-release"]
}

target "service" {
  dockerfile = "docker/service.Dockerfile"
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
  platforms = ["linux/amd64", "linux/arm64"]
  output    = ["type=registry"]
}
