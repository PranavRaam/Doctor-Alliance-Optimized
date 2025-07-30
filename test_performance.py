#!/usr/bin/env python3
"""
Performance test script for supremesheet.py optimizations
"""

import time
import subprocess
import sys
import os

def run_test(test_name, command):
    """Run a test and measure execution time"""
    print(f"\n{'='*60}")
    print(f"Running: {test_name}")
    print(f"Command: {' '.join(command)}")
    print(f"{'='*60}")
    
    start_time = time.time()
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=3600)  # 1 hour timeout
        end_time = time.time()
        
        if result.returncode == 0:
            print(f"✅ SUCCESS: {test_name}")
            print(f"⏱️  Execution time: {end_time - start_time:.2f} seconds")
            print(f"📊 Output:")
            print(result.stdout[-1000:])  # Last 1000 chars of output
        else:
            print(f"❌ FAILED: {test_name}")
            print(f"Error: {result.stderr}")
            
        return end_time - start_time, result.returncode == 0
        
    except subprocess.TimeoutExpired:
        print(f"⏰ TIMEOUT: {test_name} took longer than 1 hour")
        return None, False
    except Exception as e:
        print(f"💥 ERROR: {test_name} - {e}")
        return None, False

def main():
    if not os.path.exists("doctoralliance_combined_output.xlsx"):
        print("❌ Input file 'doctoralliance_combined_output.xlsx' not found!")
        print("Please ensure the input file exists before running tests.")
        return
    
    tests = [
        {
            "name": "Optimized with 3 workers, pre-fetch enabled",
            "command": ["python", "supremesheet.py", "doctoralliance_combined_output.xlsx", "test_output_3workers.xlsx", "3", "true"]
        },
        {
            "name": "Optimized with 5 workers, pre-fetch enabled", 
            "command": ["python", "supremesheet.py", "doctoralliance_combined_output.xlsx", "test_output_5workers.xlsx", "5", "true"]
        },
        {
            "name": "Optimized with 10 workers, pre-fetch enabled",
            "command": ["python", "supremesheet.py", "doctoralliance_combined_output.xlsx", "test_output_10workers.xlsx", "10", "true"]
        },
        {
            "name": "Optimized with 5 workers, pre-fetch disabled",
            "command": ["python", "supremesheet.py", "doctoralliance_combined_output.xlsx", "test_output_no_prefetch.xlsx", "5", "false"]
        }
    ]
    
    results = []
    
    for test in tests:
        execution_time, success = run_test(test["name"], test["command"])
        results.append({
            "name": test["name"],
            "time": execution_time,
            "success": success
        })
    
    # Print summary
    print(f"\n{'='*60}")
    print("PERFORMANCE TEST SUMMARY")
    print(f"{'='*60}")
    
    successful_tests = [r for r in results if r["success"] and r["time"] is not None]
    
    if successful_tests:
        fastest = min(successful_tests, key=lambda x: x["time"])
        slowest = max(successful_tests, key=lambda x: x["time"])
        
        print(f"🏆 Fastest: {fastest['name']} ({fastest['time']:.2f}s)")
        print(f"🐌 Slowest: {slowest['name']} ({slowest['time']:.2f}s)")
        
        if len(successful_tests) > 1:
            speedup = slowest["time"] / fastest["time"]
            print(f"⚡ Speedup: {speedup:.2f}x faster than slowest")
    
    print(f"\n📋 All Results:")
    for result in results:
        status = "✅" if result["success"] else "❌"
        time_str = f"{result['time']:.2f}s" if result["time"] is not None else "TIMEOUT/ERROR"
        print(f"   {status} {result['name']}: {time_str}")

if __name__ == "__main__":
    main() 