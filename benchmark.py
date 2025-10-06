#!/usr/bin/env python3
"""
P2P Transfer Performance Benchmark

Cross-platform benchmark script for testing sequential vs windowed transfer performance.

Usage:
    # Run as receiver (listens for incoming transfers)
    python benchmark.py --mode receiver --port 7779

    # Run as sender to localhost (auto-starts receiver)
    python benchmark.py --mode sender

    # Run as sender to remote machine
    python benchmark.py --mode sender --receiver-ip 192.168.1.100 --port 7779
"""

import argparse
import json
import os
import platform
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional


@dataclass
class BenchmarkResult:
    """Single benchmark test result"""
    test_name: str
    file_size: int
    duration: float
    throughput: float  # MB/s


class BenchmarkRunner:
    def __init__(self, binary_path: str, port: int, receiver_ip: str, output_file: str):
        self.binary_path = binary_path
        self.port = port
        self.receiver_ip = receiver_ip
        self.output_file = output_file
        self.is_local = receiver_ip in ["127.0.0.1", "localhost"]
        self.results: List[BenchmarkResult] = []
        
        # Determine platform
        self.is_windows = platform.system() == "Windows"
        if self.is_windows:
            self.binary_path = self.binary_path.replace("/", "\\")
            if not self.binary_path.endswith(".exe"):
                self.binary_path += ".exe"
    
    def ensure_binary_exists(self) -> bool:
        """Check if binary exists, if not try to build it"""
        binary = Path(self.binary_path)
        if binary.exists():
            return True
        
        print(f"Binary not found at {self.binary_path}")
        print("Attempting to build...")
        
        try:
            subprocess.run(["cargo", "build", "--release"], check=True)
            return binary.exists()
        except subprocess.CalledProcessError:
            print("ERROR: Failed to build binary")
            return False
    
    def create_test_file(self, path: Path, size_mb: int) -> bool:
        """Create a test file with random data"""
        if path.exists():
            print(f"  ✓ Using existing: {path} ({size_mb}MB)")
            return True
        
        print(f"  Creating {size_mb}MB test file...")
        try:
            # Create parent directory
            path.parent.mkdir(parents=True, exist_ok=True)
            
            # Write random data in chunks to avoid memory issues
            chunk_size = 1024 * 1024  # 1MB chunks
            with open(path, 'wb') as f:
                for _ in range(size_mb):
                    f.write(os.urandom(chunk_size))
            
            print(f"  ✓ Created: {path}")
            return True
        except Exception as e:
            print(f"  ERROR: Failed to create test file: {e}")
            return False
    
    def run_sender_test(self, test_name: str, file_path: Path, mode: str, window_size: Optional[int]) -> Optional[BenchmarkResult]:
        """Run a single sender test"""
        print(f"\nRunning: {test_name}")
        
        # Build command
        cmd = [
            self.binary_path, "send", str(file_path),
            "--peer", f"{self.receiver_ip}:{self.port}"
        ]
        
        # Set window size: 1 for sequential, or specified value for windowed
        if mode == "windowed" and window_size:
            cmd.extend(["--window-size", str(window_size)])
        elif mode == "sequential":
            cmd.extend(["--window-size", "1"])
        
        # Measure transfer time
        start_time = time.time()
        
        try:
            result = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=300  # 5 minute timeout
            )
            
            if result.returncode != 0:
                print(f"  ERROR: Transfer failed with code {result.returncode}")
                print(f"  stderr: {result.stderr.decode()}")
                return None
            
        except subprocess.TimeoutExpired:
            print("  ERROR: Transfer timed out")
            return None
        except Exception as e:
            print(f"  ERROR: {e}")
            return None
        
        end_time = time.time()
        duration = end_time - start_time
        
        # Calculate throughput
        file_size = file_path.stat().st_size
        throughput = (file_size / duration) / (1024 * 1024)  # MB/s
        
        print(f"  Time: {duration:.2f}s, Throughput: {throughput:.2f} MB/s")
        
        return BenchmarkResult(
            test_name=test_name,
            file_size=file_size,
            duration=duration,
            throughput=throughput
        )
    
    def run_receiver_mode(self):
        """Run in receiver mode - continuously accept transfers"""
        print("=" * 50)
        print("RECEIVER MODE")
        print("=" * 50)
        print(f"Listening on port {self.port}")
        print("Waiting for incoming transfers...")
        print("Press Ctrl+C to stop")
        print()
        
        transfer_count = 0
        
        try:
            while True:
                print(f"\n[Transfer #{transfer_count + 1}] Waiting for connection...")
                
                # Start receiver
                cmd = [
                    self.binary_path, "receive",
                    "--output", "./benchmark_received",
                    "--port", str(self.port),
                    "--auto-accept"
                ]
                
                try:
                    result = subprocess.run(
                        cmd,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        timeout=600  # 10 minute timeout per transfer
                    )
                    
                    if result.returncode == 0:
                        print(f"[Transfer #{transfer_count + 1}] ✓ Complete")
                        transfer_count += 1
                        
                        # Clean up received files for next transfer
                        import shutil
                        if Path("./benchmark_received").exists():
                            shutil.rmtree("./benchmark_received")
                    else:
                        print(f"[Transfer #{transfer_count + 1}] ✗ Failed: {result.stderr.decode()}")
                        
                except subprocess.TimeoutExpired:
                    print(f"[Transfer #{transfer_count + 1}] ✗ Timeout")
                    
        except KeyboardInterrupt:
            print(f"\n\nStopped. Received {transfer_count} transfers.")
    
    def run_sender_mode(self):
        """Run in sender mode - execute benchmark tests"""
        print("=" * 50)
        print("P2P TRANSFER BENCHMARK")
        print("=" * 50)
        print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"System: {platform.system()} {platform.machine()}")
        print(f"Receiver: {self.receiver_ip}:{self.port}")
        print(f"Mode: {'Local (receiver auto-started)' if self.is_local else 'Remote (ensure receiver is running!)'}")
        print()
        
        if not self.is_local:
            print("⚠️  IMPORTANT: Start receiver on remote machine first:")
            print(f"   python benchmark.py --mode receiver --port {self.port}")
            print()
            input("Press Enter when receiver is ready...")
            print()
        
        # Create test data directory
        data_dir = Path("./benchmark_data")
        data_dir.mkdir(exist_ok=True)
        
        # Test files configuration
        test_files = {
            "10MB": data_dir / "file_10mb",
            "50MB": data_dir / "file_50mb",
            "100MB": data_dir / "file_100mb",
        }
        
        if self.is_local:
            test_files["500MB"] = data_dir / "file_500mb"
        
        # Create test files
        print("=== Test File Preparation ===")
        for name, path in test_files.items():
            size_mb = int(name.replace("MB", ""))
            if not self.create_test_file(path, size_mb):
                print(f"Failed to create {name} file, skipping...")
                del test_files[name]
        
        print()
        
        # Quick benchmark (50MB file)
        print("=== Quick Benchmark (50MB file) ===")
        file_50mb = test_files.get("50MB")
        
        if file_50mb:
            for config in [
                ("50MB Sequential", "sequential", None),
                ("50MB Windowed (4)", "windowed", 4),
                ("50MB Windowed (8)", "windowed", 8),
                ("50MB Windowed (16)", "windowed", 16),
                ("50MB Windowed (32)", "windowed", 32),
            ]:
                receiver_proc = None
                if self.is_local:
                    # Auto-start receiver for local tests
                    receiver_proc = self.start_local_receiver()
                    time.sleep(2)
                
                result = self.run_sender_test(config[0], file_50mb, config[1], config[2])
                if result:
                    self.results.append(result)
                
                if receiver_proc:
                    self.stop_local_receiver(receiver_proc)
                    time.sleep(1)
        
        # Full benchmark (local only)
        if self.is_local:
            print("\n=== Full Benchmark (Local Only) ===")
            
            full_tests = [
                ("10MB Sequential", test_files.get("10MB"), "sequential", None),
                ("10MB Windowed (16)", test_files.get("10MB"), "windowed", 16),
                ("100MB Sequential", test_files.get("100MB"), "sequential", None),
                ("100MB Windowed (16)", test_files.get("100MB"), "windowed", 16),
                ("500MB Windowed (16)", test_files.get("500MB"), "windowed", 16),
            ]
            
            for test_name, file_path, mode, window_size in full_tests:
                if not file_path:
                    continue
                
                receiver_proc = self.start_local_receiver()
                time.sleep(2)
                
                result = self.run_sender_test(test_name, file_path, mode, window_size)
                if result:
                    self.results.append(result)
                
                self.stop_local_receiver(receiver_proc)
                time.sleep(1)
        
        # Save results
        self.save_results()
        self.print_summary()
    
    def start_local_receiver(self) -> subprocess.Popen:
        """Start receiver in background (local mode only)"""
        cmd = [
            self.binary_path, "receive",
            "--output", "./benchmark_received",
            "--port", str(self.port),
            "--auto-accept"
        ]
        
        return subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
    
    def stop_local_receiver(self, proc: subprocess.Popen):
        """Stop local receiver"""
        try:
            proc.terminate()
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
    
    def save_results(self):
        """Save results to file"""
        with open(self.output_file, 'w') as f:
            f.write("=" * 50 + "\n")
            f.write("P2P TRANSFER BENCHMARK RESULTS\n")
            f.write("=" * 50 + "\n\n")
            f.write(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"System: {platform.system()} {platform.machine()}\n")
            f.write(f"Receiver: {self.receiver_ip}:{self.port}\n")
            f.write(f"Mode: {'Local' if self.is_local else 'Remote'}\n\n")
            
            f.write("-" * 80 + "\n")
            f.write(f"{'Test Name':<30} {'File Size':<15} {'Duration':<15} {'Throughput'}\n")
            f.write("-" * 80 + "\n")
            
            for result in self.results:
                size_mb = result.file_size / (1024 * 1024)
                f.write(f"{result.test_name:<30} {size_mb:>10.2f} MB   {result.duration:>8.2f}s      {result.throughput:>8.2f} MB/s\n")
            
            f.write("-" * 80 + "\n")
        
        print(f"\nResults saved to: {self.output_file}")
    
    def print_summary(self):
        """Print summary of results"""
        print("\n" + "=" * 50)
        print("BENCHMARK COMPLETE")
        print("=" * 50)
        print(f"\nTotal tests run: {len(self.results)}")
        
        if self.results:
            avg_throughput = sum(r.throughput for r in self.results) / len(self.results)
            max_throughput = max(r.throughput for r in self.results)
            
            print(f"Average throughput: {avg_throughput:.2f} MB/s")
            print(f"Peak throughput: {max_throughput:.2f} MB/s")
            
            # Find best performing test
            best = max(self.results, key=lambda r: r.throughput)
            print(f"Best performance: {best.test_name} ({best.throughput:.2f} MB/s)")
        
        print(f"\nDetailed results: {self.output_file}")


def main():
    parser = argparse.ArgumentParser(
        description="P2P Transfer Performance Benchmark",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run as receiver (on remote machine)
  python benchmark.py --mode receiver --port 7779
  
  # Run as sender to localhost (auto-starts receiver)
  python benchmark.py --mode sender
  
  # Run as sender to remote machine
  python benchmark.py --mode sender --receiver-ip 192.168.1.100
        """
    )
    
    parser.add_argument(
        "--mode",
        choices=["sender", "receiver"],
        required=True,
        help="Run mode: sender (runs tests) or receiver (accepts transfers)"
    )
    
    parser.add_argument(
        "--receiver-ip",
        default="127.0.0.1",
        help="Receiver IP address (sender mode only, default: 127.0.0.1)"
    )
    
    parser.add_argument(
        "--port",
        type=int,
        default=7779,
        help="Port number (default: 7779)"
    )
    
    parser.add_argument(
        "--binary",
        default="./target/release/p2p-transfer",
        help="Path to p2p-transfer binary (default: ./target/release/p2p-transfer)"
    )
    
    parser.add_argument(
        "--output",
        default="benchmark_results.txt",
        help="Output file for results (sender mode only, default: benchmark_results.txt)"
    )
    
    args = parser.parse_args()
    
    # Create runner
    runner = BenchmarkRunner(
        binary_path=args.binary,
        port=args.port,
        receiver_ip=args.receiver_ip,
        output_file=args.output
    )
    
    # Check binary exists
    if not runner.ensure_binary_exists():
        print("ERROR: Binary not found and build failed")
        sys.exit(1)
    
    # Run appropriate mode
    try:
        if args.mode == "receiver":
            runner.run_receiver_mode()
        else:
            runner.run_sender_mode()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
