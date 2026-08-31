#!/usr/bin/env bash
# 构建、发布多架构 rclone-filestore OCI 制品。
#
# 每个架构都是原生构建（不使用 QEMU 模拟）；publish 只组装已经通过验证的
# 单架构镜像，不重新构建。不使用 Makefile/just 编排——本脚本就是唯一入口,
# 直接对着 Dockerfile.filestore 操作,不挂在上游 rclone 的 Makefile 里,
# 避免和上游自己的 TAG/RELEASE_TAG 变量共享命名空间。
set -euo pipefail

readonly script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
readonly repository_root="$(cd "${script_dir}/.." && pwd -P)"

# 两种架构共用的不可变多平台 Go 工具链镜像和编译参数。
readonly GO_IMAGE='docker.io/library/golang@sha256:e17419604b6d1f9bc245694425f0ec9b1b53685c80850900a376fb10cb0f70cb'
readonly GO_VERSION='1.25.4'
readonly GOAMD64_LEVEL='v3'
readonly GOARM64_LEVEL='v8.0'
readonly LOCAL_IMAGE_REPOSITORY='oma-local/rclone-filestore'
# artifact stage 内二进制的路径,publish 要从容器里把它复制出来做校验。
readonly ARTIFACT_BINARY_PATH='/opt/rclone/rclone-filestore'

usage() {
  cat >&2 <<'USAGE'
usage: scripts/build-image.sh <command> [argument ...]

commands:
  build [amd64|arm64|all]                        build and verify one or all platforms (default: all)
  publish <registry/repository> [tag]            promote verified local images into one OCI index
                                                  (tag defaults to <commit-date>-<short-sha>)
  smoke                                           exercise publish against a disposable local registry
USAGE
}

require_tools() {
  command -v docker >/dev/null || { echo "docker is required" >&2; exit 1; }
  docker buildx version >/dev/null
}

git_revision() {
  git -C "$repository_root" rev-parse HEAD
}

git_dirty() {
  [[ -n "$(git -C "$repository_root" status --porcelain)" ]]
}

# 构建和发布 tag 格式：<构建日期>-<7 位 short sha>,例如 20260831-abfedff。
# 刻意不使用上游 rclone Makefile 里的 TAG 变量——那个变量编码的是上游自己的
# VERSION/beta 计数器,和这个制品的发布版本毫无关系。
default_release_tag() {
  local revision build_date
  revision="$(git_revision)"
  build_date="$(date +%Y%m%d)"
  printf '%s-%s\n' "$build_date" "${revision:0:7}"
}

# 写入镜像 revision label 的值。工作树脏时打上 -dirty 后缀,
# 这样 publish 才能识别出"这个本地镜像不是从干净提交构建的"并拒绝正式发布。
build_git_hash() {
  local revision
  revision="$(git_revision)"
  if git_dirty; then
    printf '%s-dirty\n' "$revision"
  else
    printf '%s\n' "$revision"
  fi
}

# 本地单架构标签、OCI version label 和最终发布标签使用同一个版本号。
image_version() {
  default_release_tag
}

# 本地单架构镜像标签,build 和 publish 共用同一套命名规则,
# 保证 publish 总能找到 build 刚刚产出的镜像。
local_image_tag() {
  printf '%s:%s-%s\n' "$LOCAL_IMAGE_REPOSITORY" "$(image_version)" "$1"
}

# 用来跑二进制探针的验证镜像标签。它复用同一个已固定的 GO_IMAGE digest，
# 并作为独立 stage 加载；只用于本地验证，不进入最终 artifact。
verification_image_tag() {
  printf 'oma-local/rclone-filestore-verification:%s-%s\n' "$(image_version)" "$1"
}

# ── build：原生构建并验证一个或多个架构 ──────────────────────
#
# 每个架构构建两个 target：
#   artifact              - scratch 基础上的纯二进制制品,发布出去的就是它
#                            （单元测试和 ELF/GLIBC 检查已经内嵌在这个 target
#                            的构建过程里,详见 Dockerfile.filestore)
#   verification-runtime   - 独立的运行时镜像,用来跑 artifact 里提取出的二进制
cmd_build() {
  local requested_arch="${1:-all}" arches

  case "$requested_arch" in
    amd64|arm64) arches=("$requested_arch") ;;
    all) arches=(amd64 arm64) ;;
    *) usage; exit 2 ;;
  esac

  require_tools

  local version build_time hash
  version="$(image_version)"
  build_time="$(git -C "$repository_root" show -s --format=%cI HEAD)"
  hash="$(build_git_hash)"

  local arch platform local_image verification_image
  for arch in "${arches[@]}"; do
    platform="linux/${arch}"
    local_image="$(local_image_tag "$arch")"
    verification_image="$(verification_image_tag "$arch")"

    docker buildx build \
      --platform "$platform" \
      --file "${repository_root}/Dockerfile.filestore" \
      --build-arg "GO_IMAGE=${GO_IMAGE}" \
      --build-arg "GO_VERSION=${GO_VERSION}" \
      --build-arg "GOAMD64_LEVEL=${GOAMD64_LEVEL}" \
      --build-arg "GOARM64_LEVEL=${GOARM64_LEVEL}" \
      --build-arg "RCLONE_FILESTORE_VERSION=${version}" \
      --build-arg "BUILD_GIT_HASH=${hash}" \
      --build-arg "BUILD_TIME=${build_time}" \
      --target artifact --provenance=false \
      --tag "$local_image" --load "$repository_root"
    docker buildx build \
      --platform "$platform" \
      --file "${repository_root}/Dockerfile.filestore" \
      --build-arg "GO_IMAGE=${GO_IMAGE}" \
      --target verification-runtime --provenance=false \
      --tag "$verification_image" --load "$repository_root"

    local image_arch
    image_arch="$(docker image inspect "$local_image" --format '{{.Architecture}}')"
    [[ "$image_arch" == "$arch" ]] || {
      echo "image architecture mismatch: expected $arch, got $image_arch" >&2
      exit 1
    }
    echo "built and verified $platform: $local_image"
  done
}

# ── publish：把已验证的本地镜像组装成一个多架构 OCI 索引 ──────
#
# 不重新构建、不重新验证——只信任 build 已经做过的检查结果,
# 逐架构推送单架构镜像、再用 imagetools create 合成统一标签。
cmd_publish() (
  local image_repository="${1:-}" release_tag="${2:-}"

  [[ -n "$image_repository" ]] || {
    echo "usage: scripts/build-image.sh publish <registry/repository> [tag]" >&2
    exit 2
  }
  [[ -n "$release_tag" ]] || release_tag="$(default_release_tag)"

  require_tools
  command -v jq >/dev/null || { echo "jq is required" >&2; exit 1; }

  local revision
  revision="$(git_revision)"
  # 默认拒绝发布脏工作树；只有本地一次性冒烟测试（cmd_smoke)会显式放行。
  if git_dirty && [[ "${ALLOW_DIRTY_PUBLISH:-0}" != 1 ]]; then
    echo "refusing to publish a dirty worktree; set ALLOW_DIRTY_PUBLISH=1 only for a disposable local registry smoke test" >&2
    exit 1
  fi

  local -a child_refs=() summary_lines=()
  local work_dir
  work_dir="$(mktemp -d)"
  trap 'rm -rf "$work_dir"' EXIT

  local arch platform local_image verification_image binary_output
  local image_arch image_revision container_id binary_sha256 platform_tag digest
  for arch in amd64 arm64; do
    platform="linux/${arch}"
    local_image="$(local_image_tag "$arch")"
    verification_image="$(verification_image_tag "$arch")"
    binary_output="${work_dir}/rclone-filestore-${arch}"

    image_arch="$(docker image inspect "$local_image" --format '{{.Architecture}}')"
    [[ "$image_arch" == "$arch" ]] || {
      echo "image architecture mismatch: expected $arch, got $image_arch" >&2
      exit 1
    }
    # 核对镜像 label 里记录的 revision,确保发布的是当前 HEAD 构建出的镜像,
    # 而不是本地缓存里某个更早提交的产物。
    image_revision="$(docker image inspect "$local_image" \
      --format '{{index .Config.Labels "org.opencontainers.image.revision"}}')"
    if [[ "${ALLOW_DIRTY_PUBLISH:-0}" == 1 ]]; then
      [[ "${image_revision%-dirty}" == "$revision" ]] || {
        echo "image revision mismatch: $local_image" >&2; exit 1;
      }
    elif [[ "$image_revision" != "$revision" ]]; then
      echo "image was not built from the clean current revision: $local_image" >&2
      exit 1
    fi

    # 提取二进制、算 SHA-256,在 verification-runtime 镜像里跑一次 --help,
    # 确认这个 artifact 镜像里实际打包的字节能正常执行。
    container_id="$(docker create --platform "$platform" \
      --entrypoint "$ARTIFACT_BINARY_PATH" "$local_image" --help)"
    docker cp "${container_id}:${ARTIFACT_BINARY_PATH}" "$binary_output"
    docker rm "$container_id" >/dev/null
    chmod 0755 "$binary_output"
    binary_sha256="$(sha256sum "$binary_output" | awk '{print $1}')"
    docker run --rm --platform "$platform" \
      --volume "${binary_output}:/candidate/rclone-filestore:ro" \
      --entrypoint /candidate/rclone-filestore \
      "$verification_image" --help >/dev/null

    # 推送单架构子标签,记录推送后 Registry 返回的不可变 digest——
    # 后面组装索引只使用这个 digest,不使用可变的 <tag>-<arch> 标签本身。
    platform_tag="${image_repository}:${release_tag}-${arch}"
    docker image tag "$local_image" "$platform_tag"
    docker image push "$platform_tag"
    digest="$(docker buildx imagetools inspect "$platform_tag" | sed -n 's/^Digest:[[:space:]]*//p' | head -n 1)"
    [[ "$digest" =~ ^sha256:[0-9a-f]{64}$ ]] || { echo "unable to resolve digest for $platform_tag" >&2; exit 1; }
    child_refs+=("${image_repository}@${digest}")
    summary_lines+=("linux/${arch}: image=${image_repository}@${digest} binary_sha256=${binary_sha256}")
  done

  # 用不可变 digest 组装统一标签,再读回来确认平台集合恰好是 amd64+arm64。
  local index_ref index_raw actual_platforms
  index_ref="${image_repository}:${release_tag}"
  docker buildx imagetools create --tag "$index_ref" "${child_refs[@]}"
  index_raw="$(docker buildx imagetools inspect --raw "$index_ref")"
  actual_platforms="$(jq -r '[.manifests[] | select(.platform.os == "linux") | .platform.architecture] | sort | join(" ")' <<<"$index_raw")"
  [[ "$actual_platforms" == 'amd64 arm64' ]] || {
    echo "published index has unexpected platforms: $actual_platforms" >&2
    exit 1
  }

  echo "published multi-architecture index ${index_ref}"
  printf '  %s\n' "${summary_lines[@]}"
)

# ── smoke：对着一次性本地 Registry 走一遍真实的 publish 流程 ──
#
# 用来验证 push + imagetools create 这条路径本身没问题,
# 不需要连正式 Registry,容器退出时临时 registry 自动清理。
cmd_smoke() (
  local registry_name="oma-rclone-filestore-registry-$$"
  cleanup() {
    docker rm -f "$registry_name" >/dev/null 2>&1 || true
  }
  trap cleanup EXIT

  docker run --detach --rm --publish 127.0.0.1::5000 --name "$registry_name" registry:2 >/dev/null
  local registry_port
  registry_port="$(docker port "$registry_name" 5000/tcp | sed -n 's/.*://p' | head -n 1)"
  [[ "$registry_port" =~ ^[0-9]+$ ]] || { echo "unable to resolve local registry port" >&2; exit 1; }

  ALLOW_DIRTY_PUBLISH=1 cmd_publish "127.0.0.1:${registry_port}/oma/rclone-filestore" local-smoke
)

main() {
  local command="${1:-}"
  shift || true

  case "$command" in
    build) cmd_build "$@" ;;
    publish) cmd_publish "$@" ;;
    smoke) cmd_smoke "$@" ;;
    *) usage; exit 2 ;;
  esac
}

main "$@"
