#!/bin/bash
# Demo script to show progress bars in action

echo "🎯 P2P File Transfer - Progress Bar Demo"
echo "========================================="
echo ""

# Create test directory with multiple files
echo "📁 Setting up test files..."
rm -rf demo_folder
mkdir -p demo_folder/{src,docs,data}

# Create files of various sizes
echo "Creating test files..."
dd if=/dev/zero of=demo_folder/file1.bin bs=1M count=5 2>/dev/null
dd if=/dev/zero of=demo_folder/file2.bin bs=1M count=3 2>/dev/null
dd if=/dev/zero of=demo_folder/src/main.rs bs=1K count=100 2>/dev/null
dd if=/dev/zero of=demo_folder/src/lib.rs bs=1K count=50 2>/dev/null
dd if=/dev/zero of=demo_folder/docs/README.md bs=1K count=10 2>/dev/null
dd if=/dev/zero of=demo_folder/data/config.json bs=1K count=5 2>/dev/null

echo "✅ Created 6 files in demo_folder/"
tree demo_folder/ 2>/dev/null || ls -lR demo_folder/

echo ""
echo "📊 To test progress bars, run in two terminals:"
echo ""
echo "  Terminal 1 (Receiver):"
echo "  $ ./target/release/p2p-transfer receive ./received --port 7778"
echo ""
echo "  Terminal 2 (Sender):"
echo "  $ ./target/release/p2p-transfer send ./demo_folder --to 127.0.0.1:7778"
echo ""
echo "  You should see:"
echo "  - Overall progress bar showing files completed"
echo "  - Current file progress bar showing bytes transferred"
echo "  - Elapsed time and percentages"
echo ""

# Show file structure
echo "📁 Demo folder structure:"
du -sh demo_folder/*
echo ""
echo "Total size: $(du -sh demo_folder | cut -f1)"
