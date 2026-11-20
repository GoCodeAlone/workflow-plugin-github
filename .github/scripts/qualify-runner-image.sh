#!/usr/bin/env bash
# CI-only image qualification. No registration, publication, or runtime network.
set -euo pipefail
[[ "$(uname -sm)" == "Linux x86_64" ]] || { echo "Linux/amd64 required" >&2; exit 1; }
[[ -z "${DOCKER_HOST:-}" && -z "${DOCKER_CONTEXT:-}" ]] || { echo "Local hosted Docker required" >&2; exit 1; }
for tool in git go docker timeout python3 sha256sum tar; do command -v "$tool" >/dev/null; done
: "${RUNNER_TEMP:?RUNNER_TEMP required}"
owned=$(mktemp -d "$RUNNER_TEMP/runner-image-ci.XXXXXXXX")
nonce=$(basename "$owned")
image="workflow-github-runner-ci:$nonce"
label="workflow.github.image-ci=$nonce"
deadline=$((SECONDS + 1200))
cleanup_deadline=$((deadline + 180))
cleaning=0
docker_started=0
build_complete=0
create_pending=0

bounded() {
  local limit=$1 remaining
  shift
  if ((cleaning)); then remaining=$((cleanup_deadline - SECONDS - 5))
  else remaining=$((deadline - SECONDS - 5)); fi
  ((remaining > 0)) || { echo "qualification deadline exhausted" >&2; return 124; }
  ((limit <= remaining)) || limit=$remaining
  timeout --signal=TERM --kill-after=5s "$limit" "$@"
}

cleanup() {
  local status=$? failed=0 ids id owner remaining
  trap - EXIT
  trap '' INT TERM
  set +e
  cleaning=1
  if ((docker_started)); then
    if ! ids=$(bounded 15 docker container ls --all --no-trunc --filter "label=$label" --format '{{.ID}}'); then failed=1
    else
      for id in $ids; do
        [[ "$id" =~ ^[0-9a-f]{64}$ ]] || { failed=1; continue; }
        owner=$(bounded 15 docker container inspect --format '{{ index .Config.Labels "workflow.github.image-ci" }}' "$id")
        [[ $? == 0 && "$owner" == "$nonce" ]] || { failed=1; continue; }
        bounded 15 docker container rm --force "$id" >/dev/null || failed=1
      done
    fi
    remaining=$(bounded 15 docker container ls --all --no-trunc --filter "label=$label" --format '{{.ID}}')
    [[ $? == 0 && -z "$remaining" ]] || failed=1
    if ! ids=$(bounded 15 docker image ls --all --no-trunc --filter "reference=$image" --format '{{.ID}}'); then failed=1
    elif [[ -n "$ids" ]]; then
      owner=$(bounded 15 docker image inspect --format '{{ index .Config.Labels "workflow.github.image-ci" }}' "$image")
      if [[ $? == 0 && "$owner" == "$nonce" ]]; then
        bounded 15 docker image rm "$image" >/dev/null || failed=1
      else failed=1; fi
    fi
    remaining=$(bounded 15 docker image ls --all --no-trunc --filter "label=$label" --format '{{.ID}}')
    [[ $? == 0 && -z "$remaining" ]] || failed=1
    if ((!build_complete)); then
      echo "Docker build completion unverified; daemon/cache state is not proven clean" >&2
      failed=1
    fi
    if ((create_pending)); then
      echo "Docker create completion unverified; an empty listing does not prove the request finished" >&2
      failed=1
    fi
  fi
  if ((failed)); then
    echo "owned cleanup uncertain; retained $owned and label $label" >&2
    ((status != 0)) || status=1
  else
    if rm -rf -- "$owned" && [[ ! -e "$owned" ]]; then
      echo "owned containers, tagged image and temporary files removed; builder cache/base layers are not pruned"
    else
      echo "owned file cleanup failed: $owned" >&2
      ((status != 0)) || status=1
    fi
  fi
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

mkdir -p "$owned/docker-config" "$owned/context/dist" "$owned/context/cmd/github-actions-runner-job" "$owned/build-source"
export DOCKER_CONFIG="$owned/docker-config"
# Existing CI already populated the module cache; this step cannot fetch modules
# or consult the existing credential-bearing global Git rewrite.
export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_COUNT=0
export GOWORK=off GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off GOFLAGS=
export CGO_ENABLED=0 GOMAXPROCS=2 GOMEMLIMIT=768MiB
unset GH_TOKEN GITHUB_TOKEN RELEASES_TOKEN
git diff --quiet
git diff --cached --quiet
source_commit=$(git rev-parse HEAD)
source_tree=$(git rev-parse 'HEAD^{tree}')
# Compile only tracked HEAD bytes, never untracked/ignored files from prior tests.
bounded 15 git archive --format=tar --output="$owned/source.tar" "$source_commit"
bounded 15 tar -xf "$owned/source.tar" -C "$owned/build-source"
dockerfile="$owned/build-source/cmd/github-actions-runner-job/Dockerfile"
[[ "$(sha256sum "$dockerfile" | cut -d' ' -f1)" == de4abb600bcfde35a28c996b2ebd5f811c1da6f84560a5cb81758fd536227d38 ]]
compiler=$(bounded 10 go env GOVERSION)
[[ "$compiler" == "go$(awk '$1=="go" {print $2}' "$owned/build-source/go.mod")" ]]
printf 'source_commit=%s source_tree=%s compiler=%s\n' "$source_commit" "$source_tree" "$compiler"
(
  cd "$owned/build-source"
  GOOS=linux GOARCH=amd64 bounded 300 go build -mod=readonly -p=2 -buildvcs=false \
    -o "$owned/context/dist/github-actions-runner-job-linux-amd64" ./cmd/github-actions-runner-job
)
binary_sha=$(sha256sum "$owned/context/dist/github-actions-runner-job-linux-amd64" | cut -d' ' -f1)
printf 'runner_job_sha256=%s\n' "$binary_sha"
cp "$dockerfile" "$owned/context/Dockerfile"
cp "$owned/build-source/cmd/github-actions-runner-job/entrypoint.sh" "$owned/context/cmd/github-actions-runner-job/entrypoint.sh"
# This closed context contains exactly three files: no .git, home, cache or token.
[[ $(find "$owned/context" -type f | wc -l) -eq 3 ]]
docker_started=1
bounded 600 docker build --platform linux/amd64 --pull --iidfile "$owned/image.id" \
  --label "$label" --label "org.opencontainers.image.revision=$source_commit" \
  --label "workflow.github.source-tree=$source_tree" --tag "$image" "$owned/context"
build_complete=1
image_id=$(cat "$owned/image.id")
[[ "$image_id" =~ ^sha256:[0-9a-f]{64}$ ]]
[[ "$(bounded 15 docker image inspect --format '{{.Os}}/{{.Architecture}} {{.Config.User}}' "$image_id")" == "linux/amd64 runner" ]]
printf 'local_image_id=%s\n' "$image_id"
bounded 15 docker image inspect --format 'rootfs_layers={{json .RootFS.Layers}}' "$image_id"
common=(--label "$label" --network none --read-only --cap-drop ALL
  --security-opt no-new-privileges --cpus 2 --memory 3g --memory-swap 3g --pids-limit 128
  --tmpfs "/tmp:rw,exec,nosuid,nodev,size=512m,mode=1777"
  --tmpfs "/workspace:rw,exec,nosuid,nodev,size=2g,mode=1777")
check_runtime() {
  local cid=$1
  [[ "$cid" =~ ^[0-9a-f]{64}$ ]]
  [[ "$(bounded 15 docker container inspect --format '{{.HostConfig.ReadonlyRootfs}} {{.HostConfig.NetworkMode}} {{.HostConfig.Privileged}}' "$cid")" == "true none false" ]]
}
check_finished() {
  [[ "$(bounded 15 docker container inspect --format '{{.State.Status}} {{.State.ExitCode}} {{.State.OOMKilled}}' "$1")" == "exited 0 false" ]]
}
cat >"$owned/spec-input.json" <<'SPEC'
{"mode":"dispatch_then_wait","environment":"ci","os":"linux","worker_id":"worker1","task_id":"task1","organization":"example","repository":"example/fixture","workflow":"never-dispatched.yml","ref":"0000000000000000000000000000000000000000","runner_group":"ci-fixture","require_preflight":true}
SPEC
# Real packaged entrypoint extracts its archive and invokes --spec; this path
# returns deterministic metadata before any provider or GitHub request.
create_pending=1
spec_id=$(bounded 30 docker create -i --name "$nonce-spec" "${common[@]}" "$image_id" --spec)
check_runtime "$spec_id"
create_pending=0
bounded 120 docker start --attach --interactive "$spec_id" <"$owned/spec-input.json" >"$owned/spec-output.json"
check_finished "$spec_id"
python3 - "$owned/spec-output.json" <<'PY'
import json,sys
with open(sys.argv[1]) as stream:
    result=json.load(stream)
name="wfc-ci-ghp-linux-worker1-task1"
expected={"runner_name":name,"runner_group":"ci-fixture",
          "labels":["self-hosted","linux",name,"wfc-ghp-ci","wfc-ghp-ephemeral"]}
if result != expected:
    raise SystemExit("packaged spec did not match the synthetic request")
print("packaged entrypoint --spec passed; no runner registered")
PY
cat >"$owned/probe.sh" <<'PROBE'
set -euo pipefail
test "$(id -u)" -ne 0
test "$(go version)" = 'go version go1.27.2 linux/amd64'
test "$(sha256sum /usr/local/bin/github-actions-runner-job | cut -d' ' -f1)" = "$1"
if (: >/ci-readonly-check) 2>/dev/null; then echo "root write unexpectedly succeeded" >&2; exit 1; fi
export GOENV=off GOWORK=off GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off CGO_ENABLED=0 GOFLAGS=
export GOMAXPROCS=1 GOMEMLIMIT=256MiB GOCACHE=/tmp/go-cache GOMODCACHE=/tmp/go-modcache GOTMPDIR=/tmp/go-tmp
mkdir -p "$GOCACHE" "$GOMODCACHE" "$GOTMPDIR" /workspace/probe
cd /workspace/probe
printf 'module example.invalid/imageprobe\n\ngo 1.26.0\n' >go.mod
printf 'package imageprobe\nfunc Sum() int { return 1+2+3 }\n' >sum.go
printf 'package imageprobe\nimport "testing"\nfunc TestSum(t *testing.T) { if Sum()!=6 { t.Fatal("sum mismatch") } }\n' >sum_test.go
go test -p=1 -count=1 ./...
printf 'package imageprobe\nfunc Sum() int { return missingValue }\n' >sum.go
if go build -p=1 ./... >/tmp/ordinary-build-failure.log 2>&1; then
  echo "ordinary invalid build unexpectedly succeeded" >&2; exit 1
fi
grep -q 'undefined: missingValue' /tmp/ordinary-build-failure.log
printf 'image Go version, source binary, non-root/read-only and success/failure probes passed; uid=%s\n' "$(id -u)"
PROBE
create_pending=1
probe_id=$(bounded 30 docker create -i --name "$nonce-go" "${common[@]}" \
  --entrypoint /bin/bash "$image_id" -s -- "$binary_sha")
check_runtime "$probe_id"
create_pending=0
bounded 180 docker start --attach --interactive "$probe_id" <"$owned/probe.sh"
check_finished "$probe_id"
echo "image qualification body passed; owned cleanup must still succeed"
