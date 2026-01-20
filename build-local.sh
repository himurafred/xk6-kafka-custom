#!/bin/bash
# Build xk6-kafka locally for faster development

set -e

export PATH=$HOME/go/bin:$HOME/go-workspace/bin:$PATH
export GOPATH=$HOME/go-workspace

echo "========================================"
echo "Building xk6-kafka Locally"
echo "========================================"
echo "Output: k6-local"
echo ""

xk6 build --with github.com/mostafa/xk6-kafka=. --output ./k6-local

echo ""
echo "✅ Build complete: ./k6-local"
echo ""
echo "To use: ./k6-local run /path/to/script.js"
