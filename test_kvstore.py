import os
import time
import threading
import subprocess
import signal
import pytest
from client import KVClient, ServerError

# Helper to start/stop server
class ServerProcess:
    def __init__(self):
        self.proc = None
        
    def start(self):
        if self.proc:
            self.stop()
        self.proc = subprocess.Popen(
            ['python3', 'server.py'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        time.sleep(1.5)  # Wait for server to start
        
    def stop(self):
        if self.proc:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
            self.proc = None
            time.sleep(0.5)  # Give OS time to release port
            
    def kill(self):
        if self.proc:
            self.proc.kill()
            self.proc.wait()
            self.proc = None

@pytest.fixture(scope="function")
def server():
    # Clean up db file before each test
    time.sleep(0.2)  # Give OS time to release files
    for fname in ['kvstore.db', 'kvstore.db.tmp']:
        try:
            if os.path.exists(fname):
                os.remove(fname)
        except Exception as e:
            print(f"Warning: Could not remove {fname}: {e}")
        
    proc = ServerProcess()
    proc.start()
    yield proc
    proc.stop()
    
    # Clean up after test
    time.sleep(0.2)  # Give OS time to release files
    for fname in ['kvstore.db', 'kvstore.db.tmp']:
        try:
            if os.path.exists(fname):
                os.remove(fname)
        except Exception as e:
            print(f"Warning: Could not remove {fname}: {e}")

def test_set_then_get(server):
    c = KVClient(port=65433)
    assert c.set('a', 1) == 'OK'
    assert c.get('a') == 1

def test_set_then_delete_then_get(server):
    c = KVClient(port=65433)
    c.set('a', 1)
    assert c.delete('a') == 'OK'
    assert c.get('a') is None

def test_get_without_setting(server):
    c = KVClient(port=65433)
    assert c.get('notset') is None

def test_set_then_set_same_key_then_get(server):
    c = KVClient(port=65433)
    c.set('a', 1)
    c.set('a', 2)
    assert c.get('a') == 2

def test_set_then_exit_then_get():
    # Clean up first
    if os.path.exists('kvstore.db'):
        os.remove('kvstore.db')
    if os.path.exists('kvstore.db.tmp'):
        os.remove('kvstore.db.tmp')
        
    proc = ServerProcess()
    proc.start()
    c = KVClient(port=65433)
    c.set('persist', 123)
    proc.stop()
    
    # Restart server
    proc.start()
    c2 = KVClient(port=65433)
    assert c2.get('persist') == 123
    proc.stop()
    
    # Clean up
    if os.path.exists('kvstore.db'):
        os.remove('kvstore.db')
    if os.path.exists('kvstore.db.tmp'):
        os.remove('kvstore.db.tmp')

def test_bulk_set(server):
    c = KVClient(port=65433)
    items = [(f'k{i}', i) for i in range(10)]
    assert c.bulk_set(items) == 'OK'
    for k, v in items:
        assert c.get(k) == v

def test_delete_nonexistent_key(server):
    c = KVClient(port=65433)
    assert c.delete('nonexistent') == 'NOT_FOUND'

def test_multiple_clients(server):
    """Test concurrent access from multiple clients."""
    c1 = KVClient(port=65433)
    c2 = KVClient(port=65433)
    
    c1.set('shared', 'value1')
    assert c2.get('shared') == 'value1'
    
    c2.set('shared', 'value2')
    assert c1.get('shared') == 'value2'

def test_large_value(server):
    """Test storing large values."""
    c = KVClient(port=65433)
    large_value = 'x' * 10000
    c.set('large', large_value)
    assert c.get('large') == large_value

def test_special_characters_in_keys(server):
    """Test keys with special characters."""
    c = KVClient(port=65433)
    special_keys = ['key-with-dash', 'key.with.dot', 'key_with_underscore', 'key:with:colon']
    
    for key in special_keys:
        c.set(key, f'value_{key}')
        assert c.get(key) == f'value_{key}'

def test_exists(server):
    """Test the exists command."""
    c = KVClient(port=65433)
    assert c.exists('test') == False
    c.set('test', 'value')
    assert c.exists('test') == True
    c.delete('test')
    assert c.exists('test') == False

def test_keys(server):
    """Test listing all keys."""
    c = KVClient(port=65433)
    
    # Empty store
    assert c.keys() == []
    
    # Add some keys
    c.set('key1', 'val1')
    c.set('key2', 'val2')
    c.set('key3', 'val3')
    
    keys = c.keys()
    assert len(keys) == 3
    assert set(keys) == {'key1', 'key2', 'key3'}

def test_bulk_set_empty(server):
    """Test bulk_set with empty list."""
    c = KVClient(port=65433)
    assert c.bulk_set([]) == 'OK'

def test_unicode_values(server):
    """Test storing unicode values."""
    c = KVClient(port=65433)
    c.set('unicode', '你好世界 🌍')
    assert c.get('unicode') == '你好世界 🌍'

def test_different_value_types(server):
    """Test storing different Python types."""
    c = KVClient(port=65433)
    
    test_values = [
        ('int', 42),
        ('float', 3.14),
        ('str', 'hello'),
        ('list', [1, 2, 3]),
        ('dict', {'a': 1, 'b': 2}),
        ('tuple', (1, 2, 3)),
        ('bool', True),
        ('none', None),
    ]
    
    for key, value in test_values:
        c.set(key, value)
        assert c.get(key) == value