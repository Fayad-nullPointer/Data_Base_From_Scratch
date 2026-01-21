# Key-Value Store Project

A robust, persistent key-value store with TCP-based protocol and comprehensive durability guarantees.

## Features

- **Persistent Storage**: Data survives server restarts with atomic writes and fsync
- **ACID-lite Operations**: Thread-safe with proper locking
- **TCP Protocol**: Binary protocol with length-prefixed messages
- **Rich API**: Set, Get, Delete, BulkSet, Exists, Keys operations
- **Error Handling**: Comprehensive error handling with retries on client
- **Durability**: Atomic file operations using temp files and rename
- **Test Suite**: Full pytest test coverage
- **Benchmarks**: Performance and durability testing tools

## Architecture Improvements

### Server Enhancements
- **Atomic Writes**: Uses temp file + rename for crash consistency
- **fsync Support**: Ensures data is persisted to disk
- **Length-Prefixed Protocol**: Handles large messages reliably
- **RLock**: Reentrant locking for better thread safety
- **Graceful Shutdown**: Proper cleanup and signal handling
- **Input Validation**: Validates all requests before processing
- **Message Size Limits**: Prevents memory exhaustion attacks
- **Logging**: Comprehensive logging for debugging

### Client Enhancements
- **Automatic Retries**: Exponential backoff for failed connections
- **Timeout Support**: Configurable timeout for all operations
- **Error Classes**: Custom exceptions for better error handling
- **Length-Prefixed Protocol**: Matches server protocol
- **Connection Pooling Ready**: Architecture supports future pooling

### Protocol
```
Message Format:
[4 bytes: message length][N bytes: pickled data]

Request: {'cmd': 'SET', 'key': 'foo', 'value': 'bar'}
Response: 'OK' or {'error': 'ERROR_CODE'}
```

## Installation

```bash
# No external dependencies required (uses standard library)
python3 -m pytest  # Optional: for running tests
```

## Usage

### Start the Server

```bash
python3 kvstore/server.py
```

The server will start on `127.0.0.1:65433` by default.

### Use the Client

```python
from kvstore.client import KVClient

# Create client with optional configuration
c = KVClient(
    host='127.0.0.1',
    port=65433,
    timeout=5.0,      # Connection timeout
    max_retries=3     # Retry failed connections
)

# Basic operations
c.set('foo', 'bar')
print(c.get('foo'))  # Output: 'bar'
c.delete('foo')

# Bulk operations
c.bulk_set([('a', 1), ('b', 2), ('c', 3)])

# Check existence
if c.exists('a'):
    print("Key 'a' exists")

# List all keys
all_keys = c.keys()
print(f"Total keys: {len(all_keys)}")

# Complex data types
c.set('user', {'name': 'Alice', 'age': 30})
c.set('numbers', [1, 2, 3, 4, 5])
```

### Run Tests

```bash
# Run all tests
pytest kvstore/test_kvstore.py

# Run with verbose output
pytest kvstore/test_kvstore.py -v

# Run specific test
pytest kvstore/test_kvstore.py::test_set_then_get
```

### Run Benchmarks

```bash
python3 kvstore/benchmark_kvstore.py
```

Expected output:
```
=== Write Throughput Test ===
Write throughput: 150-300 ops/sec
Average latency: 3-6 ms/op

=== Read Throughput Test ===
Read throughput: 200-400 ops/sec
Average latency: 2-5 ms/op

=== Durability Test ===
Durability rate: 95-100%
```

## Performance Characteristics

### Current Performance
- **Write Throughput**: ~150-300 ops/sec (limited by fsync)
- **Read Throughput**: ~200-400 ops/sec
- **Bulk Operations**: 10-20x faster than individual operations
- **Durability**: 95-100% of acknowledged writes survive crashes

### Performance Notes
- Each write operation includes an fsync for durability
- For higher throughput, consider:
  - Write-ahead logging (WAL)
  - Group commits / batching
  - Async replication
- Network latency affects performance more than disk I/O for small values

## Durability Guarantees

### What's Guaranteed
✅ Atomic writes using temp file + rename  
✅ fsync ensures data reaches disk  
✅ Thread-safe operations with proper locking  
✅ Survives process crashes and kills  
✅ Handles corrupted data files gracefully  

### What's NOT Guaranteed
❌ Distributed consistency (single server)  
❌ Protection against disk failure (no replication)  
❌ Transaction support (no multi-key ACID)  

## API Reference

### Client Methods

| Method | Parameters | Returns | Description |
|--------|-----------|---------|-------------|
| `set(key, value)` | key: any, value: any | 'OK' | Store a key-value pair |
| `get(key)` | key: any | value or None | Retrieve a value |
| `delete(key)` | key: any | 'OK' or 'NOT_FOUND' | Delete a key |
| `bulk_set(items)` | items: list of tuples | 'OK' | Store multiple pairs |
| `exists(key)` | key: any | bool | Check if key exists |
| `keys()` | - | list | Get all keys |

### Error Handling

```python
from kvstore.client import KVClient, ServerError

try:
    c = KVClient()
    c.set('key', 'value')
except ConnectionError as e:
    print(f"Cannot connect to server: {e}")
except ServerError as e:
    print(f"Server error: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")
```

## Optimization Ideas

### For Higher Throughput
1. **Write-Ahead Log (WAL)**: Buffer writes in memory, persist in batches
2. **Group Commits**: Bundle multiple operations into single fsync
3. **Read Caching**: Keep hot keys in memory
4. **Connection Pooling**: Reuse TCP connections

### For Better Scalability
1. **Sharding**: Distribute keys across multiple servers
2. **Replication**: Add replicas for read scaling and availability
3. **Consistent Hashing**: Enable dynamic server addition/removal

### For Production Use
1. **Authentication**: Add user/password support
2. **Encryption**: TLS for network traffic, encryption at rest
3. **Monitoring**: Metrics export (Prometheus, StatsD)
4. **Admin Interface**: Health checks, stats endpoint
5. **Backup/Restore**: Snapshot and restore functionality

## Troubleshooting

### Server won't start
- Check if port 65433 is already in use: `lsof -i :65433`
- Try a different port in both server and client

### Connection refused
- Ensure server is running: `ps aux | grep server.py`
- Check firewall settings
- Verify host/port in client matches server

### Data loss after crash
- Ensure disk is not full
- Check file permissions for kvstore.db
- Review server logs for fsync errors

### Poor performance
- Use `bulk_set()` instead of multiple `set()` calls
- Consider batching operations
- Check network latency with `ping`

## License

MIT License - feel free to use in your projects!

## Contributing

Contributions welcome! Areas for improvement:
- HTTP/REST API support
- More sophisticated persistence (LSM tree, B-tree)
- Distributed mode with Raft consensus
- Metrics and monitoring integration
- Compression support# Data_Base_From_Scratch
