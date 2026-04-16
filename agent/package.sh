#!/bin/bash
#
# Package deepflow-agent into a distributable tar.gz
#
# Usage:
#   ./package.sh [OPTIONS]
#
# Options:
#   -v VERSION    Version string (default: from git tag or "0.0.0")
#   -a ARCH       Architecture: x86_64 or aarch64 (default: auto-detect)
#   -b BINARY     Path to deepflow-agent binary (default: target/release/deepflow-agent)
#   -o OUTPUT_DIR Output directory for the tar.gz (default: current directory)
#   -h            Show this help
#
# Output:
#   deepflow-<version>-<date>-Linux.<glibc>.<arch>.tar.gz
#   x86_64: GLIBC2.12, aarch64: GLIBC2.17
#
# Archive contents:
#   deepflow/deepflow-agent
#   deepflow/VERSION

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Defaults
VERSION=""
ARCH=""
BINARY=""
OUTPUT_DIR="."

usage() {
    sed -n '3,19p' "$0" | sed 's/^# \?//'
    exit 0
}

while getopts "v:a:b:o:h" opt; do
    case $opt in
        v) VERSION="$OPTARG" ;;
        a) ARCH="$OPTARG" ;;
        b) BINARY="$OPTARG" ;;
        o) OUTPUT_DIR="$OPTARG" ;;
        h) usage ;;
        *) usage ;;
    esac
done

# Resolve version: -v flag > git describe > fallback
if [ -z "$VERSION" ]; then
    VERSION=$(git describe --tags --abbrev=0 2>/dev/null || echo "0.0.0")
    # Strip leading 'v' if present
    VERSION="${VERSION#v}"
fi

# 8-char commit hash
COMMIT=$(git rev-parse --short=8 HEAD)

# Build date
DATE=$(date +%Y%m%d)

# VERSION file content
VERSION_STRING="${VERSION}-${COMMIT}"

# Locate binary
if [ -z "$BINARY" ]; then
    if [ -f "target/release/deepflow-agent" ]; then
        BINARY="target/release/deepflow-agent"
    elif [ -f "target/debug/deepflow-agent" ]; then
        BINARY="target/debug/deepflow-agent"
    else
        echo "Error: deepflow-agent binary not found. Build first or specify with -b." >&2
        exit 1
    fi
fi

if [ ! -f "$BINARY" ]; then
    echo "Error: binary not found: $BINARY" >&2
    exit 1
fi

# Detect architecture from binary or system
if [ -z "$ARCH" ]; then
    BINARY_ARCH=$(file "$BINARY" 2>/dev/null | grep -oP '(x86-64|aarch64)' || true)
    case "$BINARY_ARCH" in
        x86-64)  ARCH="x86_64" ;;
        aarch64) ARCH="aarch64" ;;
        *)       ARCH=$(uname -m) ;;
    esac
fi

# GLIBC version per architecture
case "$ARCH" in
    x86_64)  GLIBC="GLIBC2.12" ;;
    aarch64) GLIBC="GLIBC2.17" ;;
    *)       GLIBC="GLIBC2.12" ;;
esac

# Archive name
ARCHIVE_NAME="deepflow-${VERSION}-${DATE}-Linux.${GLIBC}.${ARCH}.tar.gz"

# Stage files in a temp directory
STAGING=$(mktemp -d)
trap "rm -rf '$STAGING'" EXIT

mkdir -p "$STAGING/deepflow"
cp "$BINARY" "$STAGING/deepflow/deepflow-agent"
echo "$VERSION_STRING" > "$STAGING/deepflow/VERSION"

# Create tar.gz
mkdir -p "$OUTPUT_DIR"
tar -czf "$OUTPUT_DIR/$ARCHIVE_NAME" -C "$STAGING" deepflow

echo "Version:  $VERSION_STRING"
echo "Binary:   $BINARY"
echo "Package:  $OUTPUT_DIR/$ARCHIVE_NAME"
