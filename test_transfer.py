#!/usr/bin/env python3
"""
Test script for verifying P2P transfer send/receive functionality
Cross-platform compatible (Windows, macOS, Linux)
"""

import subprocess
import time
import os
import sys
import platform
import shutil
import random
import filecmp
import argparse
from pathlib import Path


def create_test_file(file_path: Path, size_mb: int = 10, compressible: bool = False):
    """Create a test file with random or compressible data"""
    if file_path.exists():
        print(f"Test file already exists: {file_path}")
        return
    
    chunk_size = 1024 * 1024  # 1MB chunks
    if compressible:
        print(f"Creating compressible test file (~50% compression ratio) ({size_mb}MB)...")
        # Create moderately compressible data by mixing repeated patterns with random data
        # This simulates real-world compressible files (e.g., text, JSON, logs)
        # Mix pattern phrase and random data at random intervals for realistic compression
        pattern_phrase = b'Lorem ipsum dolor sit amet, consectetur adipiscing elit. '
        with open(file_path, 'wb') as f:
            remaining_bytes = size_mb * chunk_size
            while remaining_bytes > 0:
                # Randomly decide: write pattern (50% chance) or random data (50% chance)
                if random.random() < 0.5:
                    # Write pattern phrase (compressible)
                    # Random length between 10-80 repetitions
                    repetitions = random.randint(10, 80)
                    data = pattern_phrase * repetitions
                else:
                    # Write random data (incompressible)
                    # Random length between 200-10000 bytes
                    data = os.urandom(random.randint(200, 10000))
                # Ensure we don't exceed the target size
                if len(data) > remaining_bytes:
                    data = data[:remaining_bytes]
                f.write(data)
                remaining_bytes -= len(data)
        print(f"✓ Created compressible test file: {file_path}")
    else:
        print(f"Creating test file ({size_mb}MB)...")
        with open(file_path, 'wb') as f:
            for _ in range(size_mb):
                f.write(os.urandom(chunk_size))
        print(f"✓ Created test file: {file_path}")


def get_binary_path() -> str:
    """Get the path to the p2p-transfer binary based on the OS"""
    system = platform.system()
    
    if system == "Windows":
        binary = Path("target/release/p2p-transfer.exe")
    else:
        binary = Path("target/release/p2p-transfer")
    
    if not binary.exists():
        print(f"❌ Binary not found: {binary}")
        print("Please build the project first: cargo build --release")
        sys.exit(1)
    
    return str(binary)


def get_file_size(file_path: Path) -> int:
    """Get file size in bytes"""
    return file_path.stat().st_size


def format_size(size_bytes: int) -> str:
    """Format bytes to human-readable size"""
    size = float(size_bytes)
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size < 1024.0:
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{size:.2f} TB"


def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Test P2P file transfer with optional bandwidth throttling')
    parser.add_argument('--size', type=int, default=10, help='Test file size in MB (default: 10)')
    parser.add_argument('--max-speed', type=str, help='Bandwidth limit (e.g., "2M", "5M", "1G")')
    parser.add_argument('--compressible', action='store_true', help='Create highly compressible test file (zeros)')
    parser.add_argument('--window-size', type=int, default=8, help='Window size for parallel transfers (1 = sequential, default: 8)')
    parser.add_argument('--port', type=int, default=14567, help='Port to use for receiver and sender (default: 14567)')
    parser.add_argument('--verbosity', type=str, default='debug', help='Verbosity level: off, error, warn, info, debug, trace (default: debug)')
    parser.add_argument('--test-reconnect', action='store_true', help='Test auto-reconnect by killing receiver mid-transfer')
    parser.add_argument('--kill-delay', type=float, default=2.0, help='Seconds to wait before killing receiver (default: 2.0)')
    parser.add_argument('--restart-delay', type=float, default=3.0, help='Seconds to wait before restarting receiver (default: 3.0)')
    args = parser.parse_args()
    
    print("=== P2P Transfer Test ===")
    if args.test_reconnect:
        print("Mode: AUTO-RECONNECT TEST")
        print(f"  Will kill receiver after {args.kill_delay}s")
        print(f"  Will restart receiver after {args.restart_delay}s")
    else:
        print("Mode: Normal transfer")
    if args.compressible:
        print("File type: Moderately compressible (~50% compression ratio)")
    else:
        print("File type: Random data (incompressible)")
    if args.max_speed:
        print(f"Bandwidth limit: {args.max_speed}")
    print(f"Transfer mode: {'Sequential' if args.window_size == 1 else f'Windowed (window size: {args.window_size})'}")
    print(f"Verbosity level: {args.verbosity}")
    print()
    
    # Setup paths
    test_file = Path("test_file")
    received_dir = Path("received")
    # The received file will have the same name as the sent file
    received_file = received_dir / test_file.name
    binary_path = get_binary_path()
    
    # Create test file
    create_test_file(test_file, size_mb=args.size, compressible=args.compressible)
    file_size = get_file_size(test_file)
    print(f"Test file size: {format_size(file_size)}")
    print()
    
    # Cleanup previous received directory
    if received_dir.exists():
        shutil.rmtree(received_dir)
    
    # Start receiver in background
    print("Starting receiver...")
    receiver_cmd = [
        binary_path, "receive",
        "--output", str(received_dir),
        "--port", str(args.port),
        "--auto-accept",
        "--verbosity", args.verbosity
    ]
    
    receiver_process = subprocess.Popen(
        receiver_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding='utf-8',
        errors='replace'
    )
    
    # Give receiver time to start
    time.sleep(2)
    
    # Check if receiver is still running
    if receiver_process.poll() is not None:
        print("❌ Receiver failed to start")
        stdout, stderr = receiver_process.communicate()
        print("STDOUT:", stdout)
        print("STDERR:", stderr)
        sys.exit(1)
    
    print("✓ Receiver started")
    print()
    
    # Start sender
    print("Starting sender...")
    sender_cmd = [
        binary_path, "send", str(test_file),
        "--peer", f"127.0.0.1:{args.port}",
        "--window-size", str(args.window_size),
        "--verbosity", args.verbosity
    ]
    
    # Add bandwidth limit if specified
    if args.max_speed:
        sender_cmd.extend(["--max-speed", args.max_speed])
        print(f"  Using bandwidth limit: {args.max_speed}")
    
    print(f"  Using window size: {args.window_size} ({'sequential' if args.window_size == 1 else 'windowed'})")
    
    # Track transfer time
    start_time = time.time()
    
    # Start sender in background if testing reconnect
    if args.test_reconnect:
        print()
        print("Starting sender in background...")
        sender_process = subprocess.Popen(
            sender_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            encoding='utf-8',
            errors='replace'
        )
        
        # Wait for transfer to start
        print(f"Waiting {args.kill_delay}s for transfer to start...")
        time.sleep(args.kill_delay)
        
        # Kill receiver to simulate connection loss
        print("🔪 Killing receiver to simulate connection loss...")
        receiver_process.terminate()
        try:
            receiver_process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            receiver_process.kill()
            receiver_process.wait()
        print("✓ Receiver killed")
        
        # Wait before restarting
        print(f"Waiting {args.restart_delay}s before restarting receiver...")
        time.sleep(args.restart_delay)
        
        # Restart receiver
        print("🔄 Restarting receiver...")
        receiver_process = subprocess.Popen(
            receiver_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            encoding='utf-8',
            errors='replace'
        )
        time.sleep(1)  # Give it time to start
        
        if receiver_process.poll() is not None:
            print("❌ Receiver failed to restart")
            sender_process.terminate()
            sys.exit(1)
        
        print("✓ Receiver restarted")
        print("Sender should auto-reconnect and resume...")
        print()
        
        # Wait for sender to complete (with longer timeout for reconnect)
        try:
            sender_process.wait(timeout=60)
            sender_exit_code = sender_process.returncode
            elapsed_time = time.time() - start_time
            
            # Get sender output
            stdout, stderr = sender_process.communicate(timeout=1)
            if stdout:
                print(stdout)
            if stderr:
                print(stderr, file=sys.stderr)
                
        except subprocess.TimeoutExpired:
            print("❌ Sender timed out after 60 seconds")
            sender_process.terminate()
            receiver_process.terminate()
            sys.exit(1)
    else:
        # Normal transfer (synchronous)
        try:
            result = subprocess.run(
                sender_cmd,
                capture_output=True,
                encoding='utf-8',
                errors='replace',
                timeout=30
            )
            sender_exit_code = result.returncode
            elapsed_time = time.time() - start_time
            
            # Print sender output for debugging
            if result.stdout:
                print(result.stdout)
            if result.stderr:
                print(result.stderr, file=sys.stderr)
            
        except subprocess.TimeoutExpired:
            print("❌ Sender timed out after 30 seconds")
            receiver_process.terminate()
            sys.exit(1)
    
    # Wait a moment for receiver to finish
    time.sleep(1)
    
    # Terminate receiver if still running
    if receiver_process.poll() is None:
        receiver_process.terminate()
        try:
            receiver_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            receiver_process.kill()
    
    print()
    print("=== Test Results ===")
    print(f"Sender exit code: {sender_exit_code}")
    print(f"Transfer time: {elapsed_time:.2f} seconds")
    
    # Calculate and display transfer speed
    if sender_exit_code == 0:
        speed_mbps = (file_size / (1024 * 1024)) / elapsed_time
        print(f"Average speed: {speed_mbps:.2f} MB/s")
    
    # Check results
    test_passed = False
    
    if sender_exit_code == 0:
        if received_file.exists():
            original_size = get_file_size(test_file)
            received_size = get_file_size(received_file)
            
            print(f"Original file size: {original_size} bytes")
            print(f"Received file size: {received_size} bytes")
            
            if original_size == received_size:
                print("✅ File sizes match!")
                
                # Verify content
                if filecmp.cmp(test_file, received_file, shallow=False):
                    print("✅ File contents match!")
                    print()
                    print("🎉 TEST PASSED!")
                    test_passed = True
                else:
                    print("❌ File contents differ!")
                    print("TEST FAILED")
            else:
                print("❌ File sizes differ!")
                print("TEST FAILED")
        else:
            print(f"❌ Received file not found: {received_file}")
            print("TEST FAILED")
    else:
        print(f"❌ Sender failed with exit code {sender_exit_code}")
        print("TEST FAILED")
    
    # Cleanup
    print()
    print("Cleaning up...")
    if received_dir.exists():
        shutil.rmtree(received_dir)
    
    # Exit with appropriate code
    sys.exit(0 if test_passed else 1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Test interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
