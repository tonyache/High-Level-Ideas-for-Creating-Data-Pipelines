#!/usr/bin/env bash
set -euo pipefail

mkdir -p build output

# Prefer clang++ if available, otherwise g++
CXX=${CXX:-clang++}
if ! command -v "$CXX" >/dev/null 2>&1; then
  if command -v g++ >/dev/null 2>&1; then
    CXX=g++
  else
    echo "No suitable C++ compiler found (need clang++ or g++)."
    exit 1
  fi
fi

$CXX -O2 -std=c++17 -o build/edgar_sessionization src/edgar_sessionization.cpp

./build/edgar_sessionization input/log.csv input/inactivity_period.txt output/sessionization.txt
echo "Wrote output/sessionization.txt"
