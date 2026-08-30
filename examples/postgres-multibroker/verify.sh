#!/bin/sh
set -eu

EXAMPLE_DIR=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
PROJECT_NAME=${BUNQUEUE_EXAMPLE_PROJECT:-bunqueue-pg-example-$(date +%s)-$$}

case "$PROJECT_NAME" in
  bunqueue-pg-example-?*) ;;
  *)
    echo "BUNQUEUE_EXAMPLE_PROJECT must start with bunqueue-pg-example-" >&2
    exit 2
    ;;
esac
case "$PROJECT_NAME" in
  *[!a-z0-9_-]*)
    echo "BUNQUEUE_EXAMPLE_PROJECT may contain only lowercase letters, digits, _ and -" >&2
    exit 2
    ;;
esac

compose() {
  docker compose --project-name "$PROJECT_NAME" --file "$EXAMPLE_DIR/compose.yaml" "$@"
}

cleanup() {
  original_status=$?
  trap - EXIT INT TERM
  set +e
  compose down --volumes --remove-orphans
  resources_status=$?
  compose down --volumes --remove-orphans --rmi local
  images_status=$?

  if [ "$resources_status" -ne 0 ] || [ "$images_status" -ne 0 ]; then
    echo "Example cleanup failed" >&2
  fi
  if [ "$original_status" -ne 0 ]; then
    exit "$original_status"
  fi
  if [ "$resources_status" -ne 0 ] || [ "$images_status" -ne 0 ]; then
    exit 1
  fi
  exit 0
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

compose config --quiet
compose --progress plain build
compose up --detach --wait --wait-timeout 120 postgres broker-a broker-b broker-c

for scenario in topology multi-queue reliability flow; do
  compose run --rm --no-deps sdk-example "$scenario"
done

compose ps
