#!/bin/bash
set -e

echo "Building Zig shared library..."

# Detect OS for library extension
if [[ "$OSTYPE" == "darwin"* ]]; then
    EXT="dylib"
elif [[ "$OSTYPE" == "linux"* ]]; then
    EXT="so"
else
    EXT="dll"
fi

# Build shared library
zig build-lib -dynamic -O ReleaseFast queue.zig -femit-bin=libqueue.$EXT

echo "Built libqueue.$EXT"
echo ""
echo "Run benchmark with:"
echo "  bun benchmark.ts"
