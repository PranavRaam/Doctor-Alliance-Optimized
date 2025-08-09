#!/usr/bin/env python3
"""
Performance monitoring script for Doctor Alliance processing pipeline.
Tracks processing speed and provides real-time feedback.
"""

import time
import psutil
import threading
from datetime import datetime
import os

class PerformanceMonitor:
    def __init__(self):
        self.start_time = None
        self.processed_count = 0
        self.total_count = 0
        self.monitoring = False
        self.monitor_thread = None
        
    def start_monitoring(self, total_count):
        """Start performance monitoring."""
        self.start_time = time.time()
        self.processed_count = 0
        self.total_count = total_count
        self.monitoring = True
        
        # Start monitoring thread
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
        
        print(f"🚀 Performance monitoring started for {total_count} items")
        
    def update_progress(self, count):
        """Update processed count."""
        self.processed_count = count
        
    def stop_monitoring(self):
        """Stop performance monitoring."""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=1)
            
    def _monitor_loop(self):
        """Background monitoring loop."""
        while self.monitoring:
            try:
                self._print_status()
                time.sleep(10)  # Update every 10 seconds
            except Exception as e:
                print(f"Monitor error: {e}")
                break
                
    def _print_status(self):
        """Print current performance status."""
        if not self.start_time or self.total_count == 0:
            return
            
        elapsed = time.time() - self.start_time
        if elapsed == 0:
            return
            
        # Calculate metrics
        rate = self.processed_count / elapsed * 60  # items per minute
        eta_minutes = (self.total_count - self.processed_count) / rate if rate > 0 else 0
        progress_pct = (self.processed_count / self.total_count) * 100
        
        # Get system resources
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        
        # Print status
        print(f"\n📊 PERFORMANCE STATUS ({datetime.now().strftime('%H:%M:%S')})")
        print(f"   Progress: {self.processed_count}/{self.total_count} ({progress_pct:.1f}%)")
        print(f"   Rate: {rate:.1f} items/minute")
        print(f"   Elapsed: {elapsed/60:.1f} minutes")
        print(f"   ETA: {eta_minutes:.1f} minutes")
        print(f"   CPU: {cpu_percent:.1f}% | Memory: {memory.percent:.1f}%")
        
    def get_final_stats(self):
        """Get final performance statistics."""
        if not self.start_time:
            return {}
            
        total_time = time.time() - self.start_time
        avg_rate = self.processed_count / total_time * 60 if total_time > 0 else 0
        
        return {
            "total_items": self.total_count,
            "processed_items": self.processed_count,
            "total_time_minutes": total_time / 60,
            "avg_rate_per_minute": avg_rate,
            "success_rate": (self.processed_count / self.total_count) * 100 if self.total_count > 0 else 0
        }

# Global monitor instance
monitor = PerformanceMonitor()

def start_monitoring(total_count):
    """Start performance monitoring."""
    monitor.start_monitoring(total_count)

def update_progress(count):
    """Update progress count."""
    monitor.update_progress(count)

def stop_monitoring():
    """Stop performance monitoring and print final stats."""
    monitor.stop_monitoring()
    stats = monitor.get_final_stats()
    
    print(f"\n🎯 FINAL PERFORMANCE STATS")
    print(f"   Total items: {stats['total_items']}")
    print(f"   Processed: {stats['processed_items']}")
    print(f"   Total time: {stats['total_time_minutes']:.1f} minutes")
    print(f"   Average rate: {stats['avg_rate_per_minute']:.1f} items/minute")
    print(f"   Success rate: {stats['success_rate']:.1f}%")
    
    return stats

if __name__ == "__main__":
    # Test the monitor
    print("Testing performance monitor...")
    start_monitoring(100)
    
    for i in range(10):
        update_progress(i * 10)
        time.sleep(2)
    
    stop_monitoring() 