#!/bin/sh
# Rebuilds Westwind from its generator, checks the seed hash, and runs every part of the toy.
# The generator and schema are research document 01a1b37a-e007-75e1-8cbc-8f029c6d397a's.
set -eu
cd "$(dirname "$0")"
mkdir -p out/westwind
cp westwind/schema.sql out/westwind/
(cd out/westwind && python3 -I ../../westwind/generate.py)
echo "6d38971dc46a9677b46d8b6b072c88fdc6af67d381b4d731316064c166e60e71  out/westwind/seed.sql" | shasum -a 256 -c
cargo build --release
bin=target/release/workers-toy
$bin correct out/westwind out | tee out/correct.txt
$bin replays out/westwind out | tee out/replays.txt
$bin rebuild out/westwind out | tee out/rebuild.txt
$bin speed out/westwind out 1000000 3 | tee out/speed.txt
