#!/bin/bash
# Demo script to test adaptive compression with compressible and incompressible data

set -e

echo "🧪 Testing Adaptive Compression"
echo "================================"
echo ""

# Build the project
echo "📦 Building project..."
cargo build --release 2>&1 | grep -E "(Compiling|Finished)" || true
echo ""

# Create test directory
TEST_DIR="/tmp/p2p-adaptive-test"
rm -rf "$TEST_DIR"
mkdir -p "$TEST_DIR"

# Create a compressible text file (1MB)
echo "📄 Creating compressible test file (1MB text)..."
for i in {1..10000}; do
    echo "This is line $i of test data. The quick brown fox jumps over the lazy dog."
done > "$TEST_DIR/compressible.txt"

# Create an incompressible file (1MB random data simulating pre-compressed)
echo "📄 Creating incompressible test file (1MB random/compressed-like data)..."
dd if=/dev/urandom of="$TEST_DIR/incompressible.bin" bs=1024 count=1024 2>/dev/null

# Get file sizes
COMP_SIZE=$(ls -lh "$TEST_DIR/compressible.txt" | awk '{print $5}')
INCOMP_SIZE=$(ls -lh "$TEST_DIR/incompressible.bin" | awk '{print $5}')

echo "  ✓ Compressible file: $COMP_SIZE"
echo "  ✓ Incompressible file: $INCOMP_SIZE"
echo ""

# Start receiver in background
echo "🎧 Starting receiver..."
RECEIVE_DIR="/tmp/p2p-adaptive-receive"
rm -rf "$RECEIVE_DIR"
mkdir -p "$RECEIVE_DIR"

./target/release/p2p-transfer receive -o "$RECEIVE_DIR" &
RECEIVER_PID=$!
sleep 2

# Test 1: Send compressible file with adaptive compression
echo ""
echo "📤 Test 1: Sending compressible file (adaptive should keep compression ON)..."
./target/release/p2p-transfer send "$TEST_DIR/compressible.txt" --to 127.0.0.1:7777 --adaptive true --compress true

echo ""
echo "  ✓ Compressible file sent with adaptive compression"
sleep 1

# Test 2: Send incompressible file with adaptive compression
echo ""
echo "📤 Test 2: Sending incompressible file (adaptive should turn compression OFF)..."
./target/release/p2p-transfer send "$TEST_DIR/incompressible.bin" --to 127.0.0.1:7777 --adaptive true --compress true

echo ""
echo "  ✓ Incompressible file sent with adaptive compression"
sleep 1

# Test 3: Send with adaptive disabled (always compress)
echo ""
echo "📤 Test 3: Sending incompressible file WITHOUT adaptive (always compress)..."
./target/release/p2p-transfer send "$TEST_DIR/incompressible.bin" --to 127.0.0.1:7777 --adaptive false --compress true

echo ""
echo "  ✓ File sent without adaptive compression"

# Kill receiver
echo ""
kill $RECEIVER_PID 2>/dev/null || true
wait $RECEIVER_PID 2>/dev/null || true

# Verify received files
echo ""
echo "✅ Verifying received files..."
if [ -f "$RECEIVE_DIR/compressible.txt" ]; then
    echo "  ✓ compressible.txt received"
fi
if [ -f "$RECEIVE_DIR/incompressible.bin" ]; then
    echo "  ✓ incompressible.bin received"
fi

echo ""
echo "🎉 Adaptive compression demo complete!"
echo ""
echo "Note: Check the transfer logs above to see when adaptive compression"
echo "      disabled compression for the incompressible file."
