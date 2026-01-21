import time
import threading
import random
import os
import subprocess
import sys
from client import KVClient

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

def benchmark_write_throughput(num_keys=1000, value_size=100):
    """Benchmark write throughput."""
    print(f"\n=== Write Throughput Test ===")
    print(f"Keys: {num_keys}, Value size: {value_size} bytes")
    
    c = KVClient()
    value = 'x' * value_size
    
    start = time.time()
    for i in range(num_keys):
        try:
            c.set(f'key{i}', value)
        except Exception as e:
            print(f"Error writing key{i}: {e}")
            return None
    end = time.time()
    
    elapsed = end - start
    throughput = num_keys / elapsed
    latency = (elapsed / num_keys) * 1000
    
    print(f"Write throughput: {throughput:.2f} ops/sec")
    print(f"Total time: {elapsed:.2f} seconds")
    print(f"Average latency: {latency:.2f} ms/op")
    
    return {
        'Test': 'Write Throughput',
        'Operations': num_keys,
        'Throughput (ops/sec)': round(throughput, 2),
        'Total Time (sec)': round(elapsed, 2),
        'Avg Latency (ms)': round(latency, 2),
        'Value Size (bytes)': value_size
    }

def benchmark_read_throughput(num_keys=1000, value_size=100):
    """Benchmark read throughput."""
    print(f"\n=== Read Throughput Test ===")
    print(f"Keys: {num_keys}, Value size: {value_size} bytes")
    
    c = KVClient()
    value = 'x' * value_size
    
    # First, populate the store
    print("Populating store...")
    for i in range(num_keys):
        c.set(f'rkey{i}', value)
    
    # Now benchmark reads
    start = time.time()
    for i in range(num_keys):
        try:
            c.get(f'rkey{i}')
        except Exception as e:
            print(f"Error reading rkey{i}: {e}")
            return None
    end = time.time()
    
    elapsed = end - start
    throughput = num_keys / elapsed
    latency = (elapsed / num_keys) * 1000
    
    print(f"Read throughput: {throughput:.2f} ops/sec")
    print(f"Total time: {elapsed:.2f} seconds")
    print(f"Average latency: {latency:.2f} ms/op")
    
    return {
        'Test': 'Read Throughput',
        'Operations': num_keys,
        'Throughput (ops/sec)': round(throughput, 2),
        'Total Time (sec)': round(elapsed, 2),
        'Avg Latency (ms)': round(latency, 2),
        'Value Size (bytes)': value_size
    }

def benchmark_bulk_write(num_operations=100, batch_size=100, value_size=100):
    """Benchmark bulk write operations."""
    print(f"\n=== Bulk Write Test ===")
    print(f"Operations: {num_operations}, Batch size: {batch_size}, Value size: {value_size} bytes")
    
    c = KVClient()
    value = 'x' * value_size
    
    start = time.time()
    for op in range(num_operations):
        items = [(f'bulk{op}_{i}', value) for i in range(batch_size)]
        try:
            c.bulk_set(items)
        except Exception as e:
            print(f"Error in bulk operation {op}: {e}")
            return None
    end = time.time()
    
    elapsed = end - start
    total_keys = num_operations * batch_size
    throughput = total_keys / elapsed
    latency = (elapsed / total_keys) * 1000
    
    print(f"Bulk write throughput: {throughput:.2f} ops/sec")
    print(f"Total keys written: {total_keys}")
    print(f"Total time: {elapsed:.2f} seconds")
    
    return {
        'Test': 'Bulk Write',
        'Operations': total_keys,
        'Throughput (ops/sec)': round(throughput, 2),
        'Total Time (sec)': round(elapsed, 2),
        'Avg Latency (ms)': round(latency, 2),
        'Value Size (bytes)': value_size
    }

def durability_test(num_keys=200, value_size=100, kill_interval=0.3):
    """Test durability under crashes."""
    print(f"\n=== Durability Test ===")
    print(f"Keys: {num_keys}, Kill interval: {kill_interval}s")
    
    # Clean up db file
    for fname in ['kvstore.db', 'kvstore.db.tmp']:
        if os.path.exists(fname):
            os.remove(fname)
    
    # Start server
    proc = subprocess.Popen(
        ['python3', 'server.py'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    time.sleep(1.5)
    
    c = KVClient()
    acked = set()
    stop_flag = threading.Event()
    write_complete = threading.Event()
    
    def writer():
        for i in range(num_keys):
            if stop_flag.is_set():
                break
            try:
                res = c.set(f'dur{i}', 'x' * value_size)
                if res == 'OK':
                    acked.add(i)
            except Exception as e:
                # Connection errors are expected during kills
                pass
            time.sleep(0.02)
        write_complete.set()
    
    def killer():
        nonlocal proc
        while not write_complete.is_set():
            time.sleep(random.uniform(0.1, kill_interval))
            if write_complete.is_set():
                break
            
            # Kill the server
            try:
                proc.kill()
                proc.wait()
            except:
                pass
            
            time.sleep(0.3)
            
            if write_complete.is_set():
                break
            
            # Restart server
            proc = subprocess.Popen(
                ['python3', 'server.py'],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            time.sleep(0.8)
    
    t1 = threading.Thread(target=writer)
    t2 = threading.Thread(target=killer)
    t1.start()
    t2.start()
    t1.join()
    write_complete.set()
    t2.join()
    
    # Clean shutdown
    try:
        proc.terminate()
        proc.wait(timeout=5)
    except:
        proc.kill()
        proc.wait()
    
    time.sleep(0.5)
    
    # Restart server and check durability
    proc = subprocess.Popen(
        ['python3', 'server.py'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    time.sleep(1.5)
    
    c2 = KVClient()
    lost = 0
    recovered = 0
    
    for i in acked:
        try:
            val = c2.get(f'dur{i}')
            if val is None:
                lost += 1
            else:
                recovered += 1
        except Exception as e:
            lost += 1
    
    durability_rate = (recovered / len(acked) * 100) if len(acked) > 0 else 0
    
    print(f"Results:")
    print(f"  Acknowledged writes: {len(acked)}")
    print(f"  Recovered: {recovered}")
    print(f"  Lost: {lost}")
    print(f"  Durability rate: {durability_rate:.2f}%")
    
    try:
        proc.terminate()
        proc.wait(timeout=5)
    except:
        proc.kill()
        proc.wait()
    
    # Clean up
    for fname in ['kvstore.db', 'kvstore.db.tmp']:
        if os.path.exists(fname):
            os.remove(fname)
    
    return {
        'Test': 'Durability',
        'Acknowledged': len(acked),
        'Recovered': recovered,
        'Lost': lost,
        'Durability Rate (%)': round(durability_rate, 2)
    }

def main():
    print("=" * 50)
    print("Key-Value Store Benchmark Suite")
    print("=" * 50)
    
    results = []
    
    try:
        # Run benchmarks and collect results
        result = benchmark_write_throughput(1000, 100)
        if result:
            results.append(result)
            
        result = benchmark_read_throughput(1000, 100)
        if result:
            results.append(result)
            
        result = benchmark_bulk_write(50, 50, 100)
        if result:
            results.append(result)
            
        result = durability_test(200, 100, 0.3)
        if result:
            results.append(result)
        
        # Display results in DataFrame
        print("\n" + "=" * 50)
        print("BENCHMARK RESULTS SUMMARY")
        print("=" * 50 + "\n")
        
        if HAS_PANDAS and results:
            # Create DataFrame for throughput tests
            throughput_results = [r for r in results if 'Throughput (ops/sec)' in r]
            if throughput_results:
                df_throughput = pd.DataFrame(throughput_results)
                print("Performance Metrics:")
                print(df_throughput.to_string(index=False))
                print()
            
            # Create DataFrame for durability test
            durability_results = [r for r in results if 'Durability Rate (%)' in r]
            if durability_results:
                df_durability = pd.DataFrame(durability_results)
                print("Durability Metrics:")
                print(df_durability.to_string(index=False))
                print()
                
        else:
            if not HAS_PANDAS:
                print("Note: Install pandas for formatted tables: pip install pandas\n")
            # Fallback display
            for result in results:
                print(result)
                print()
        
        print("=" * 50)
        print("All benchmarks completed successfully!")
        print("=" * 50)
        
    except KeyboardInterrupt:
        print("\nBenchmark interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nBenchmark failed with error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()