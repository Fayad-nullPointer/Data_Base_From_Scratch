import socket
import pickle
import struct
import time
import logging

logging.basicConfig(level=logging.WARNING)

class KVClient:
    def __init__(self, host='127.0.0.1', port=65433, timeout=5.0, max_retries=3):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.max_retries = max_retries

    def _send(self, req, retry_count=0):
        """Send request with proper error handling and retries."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(self.timeout)
                s.connect((self.host, self.port))
                
                # Serialize request
                data = pickle.dumps(req)
                
                # Send length prefix followed by data
                s.sendall(struct.pack('!I', len(data)))
                s.sendall(data)
                
                # Receive response length
                length_data = self._recv_exact(s, 4)
                if not length_data:
                    raise ConnectionError("Server closed connection")
                
                msg_length = struct.unpack('!I', length_data)[0]
                
                # Receive response data
                response_data = self._recv_exact(s, msg_length)
                if not response_data:
                    raise ConnectionError("Incomplete response from server")
                
                response = pickle.loads(response_data)
                
                # Check for server errors
                if isinstance(response, dict) and 'error' in response:
                    raise ServerError(f"Server error: {response['error']}")
                
                return response
                
        except (ConnectionRefusedError, socket.timeout, ConnectionError) as e:
            if retry_count < self.max_retries:
                logging.warning(f"Connection failed, retrying ({retry_count + 1}/{self.max_retries})...")
                time.sleep(0.5 * (retry_count + 1))  # Exponential backoff
                return self._send(req, retry_count + 1)
            else:
                raise ConnectionError(f"Failed to connect after {self.max_retries} retries: {e}")
        except Exception as e:
            raise RuntimeError(f"Unexpected error: {e}")

    def _recv_exact(self, sock, n):
        """Receive exactly n bytes from socket."""
        data = bytearray()
        while len(data) < n:
            packet = sock.recv(n - len(data))
            if not packet:
                return None
            data.extend(packet)
        return bytes(data)

    def set(self, key, value):
        """Set a key-value pair."""
        return self._send({'cmd': 'SET', 'key': key, 'value': value})

    def get(self, key):
        """Get the value for a key."""
        return self._send({'cmd': 'GET', 'key': key})

    def delete(self, key):
        """Delete a key."""
        return self._send({'cmd': 'DELETE', 'key': key})

    def bulk_set(self, items):
        """Set multiple key-value pairs at once."""
        return self._send({'cmd': 'BULK_SET', 'items': items})

    def exists(self, key):
        """Check if a key exists."""
        return self._send({'cmd': 'EXISTS', 'key': key})

    def keys(self):
        """Get all keys."""
        return self._send({'cmd': 'KEYS'})

class ServerError(Exception):
    """Exception raised when server returns an error."""
    pass