
import os
import subprocess
import sys
import time

def cleanup():
    print("=" * 50)
    print("KV Store Cleanup Script")
    print("=" * 50)
    
    # Kill any running server processes
    print("\n[1/3] Stopping any running servers...")
    try:
        result = subprocess.run(['pkill', '-f', 'server.py'], 
                              capture_output=True, text=True)
        if result.returncode == 0:
            print("  ✓ Stopped running servers")
            time.sleep(0.5)  # Give time for processes to terminate
        else:
            print("  ✓ No running servers found")
    except Exception as e:
        print(f"  ⚠ Could not check for running servers: {e}")
    
    # Remove database files
    print("\n[2/3] Removing database files...")
    files_removed = 0
    for fname in ['kvstore.db', 'kvstore.db.tmp']:
        try:
            if os.path.exists(fname):
                os.remove(fname)
                print(f"  ✓ Removed {fname}")
                files_removed += 1
            else:
                print(f"  - {fname} does not exist")
        except Exception as e:
            print(f"  ✗ Could not remove {fname}: {e}")
    
    if files_removed == 0:
        print("  ✓ No database files to clean")
    
    # Remove any test artifacts
    print("\n[3/3] Cleaning test artifacts...")
    try:
        # Remove pytest cache
        if os.path.exists('__pycache__'):
            import shutil
            shutil.rmtree('__pycache__')
            print("  ✓ Removed __pycache__")
        
        if os.path.exists('.pytest_cache'):
            import shutil
            shutil.rmtree('.pytest_cache')
            print("  ✓ Removed .pytest_cache")
    except Exception as e:
        print(f"  ⚠ Could not remove cache: {e}")
    
    print("\n" + "=" * 50)
    print("Cleanup complete! You can now run tests.")
    print("=" * 50)
    print("\nNext steps:")
    print("  • Run tests: python3 -m pytest test_kvstore.py -v")
    print("  • Run benchmarks: python3 benchmark_kvstore.py")

if __name__ == "__main__":
    try:
        cleanup()
    except KeyboardInterrupt:
        print("\n\nCleanup interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nCleanup failed: {e}")
        sys.exit(1)