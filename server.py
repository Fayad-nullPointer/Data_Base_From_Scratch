import socket
import threading
import pickle
import os
import json
import struct
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class PersistentKVStore:
    def __init__(self, filename):
        self.filename = filename
        self.lock = threading.RLock()  # Use RLock for reentrant locking
        self.data = {}
        self._load()

    def _load(self):
        if os.path.exists(self.filename):
            try:
                with open(self.filename, 'rb') as f:
                    self.data = pickle.load(f)
                logging.info(f"Loaded {len(self.data)} keys from {self.filename}")
            except (EOFError, pickle.UnpicklingError) as e:
                logging.warning(f"Failed to load data file: {e}. Starting with empty store.")
                self.data = {}
        else:
            logging.info("No existing data file found. Starting with empty store.")

    def _persist(self):
        """Persist data with atomic write using temp file and rename."""
        temp_filename = self.filename + '.tmp'
        try:
            with open(temp_filename, 'wb') as f:
                pickle.dump(self.data, f)
                f.flush()
                os.fsync(f.fileno())  # Ensure data is written to disk
            os.replace(temp_filename, self.filename)  # Atomic rename
        except Exception as e:
            logging.error(f"Failed to persist data: {e}")
            if os.path.exists(temp_filename):
                os.remove(temp_filename)
            raise

    def set(self, key, value):
        with self.lock:
            self.data[key] = value
            self._persist()
            return 'OK'

    def get(self, key):
        with self.lock:
            return self.data.get(key, None)

    def delete(self, key):
        with self.lock:
            if key in self.data:
                del self.data[key]
                self._persist()
                return 'OK'
            return 'NOT_FOUND'

    def bulk_set(self, items):
        with self.lock:
            for key, value in items:
                self.data[key] = value
            self._persist()
            return 'OK'

    def exists(self, key):
        """Check if a key exists."""
        with self.lock:
            return key in self.data

    def keys(self):
        """Return all keys."""
        with self.lock:
            return list(self.data.keys())

class KVServer:
    def __init__(self, host, port, db_file):
        self.kv = PersistentKVStore(db_file)
        self.host = host
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.running = False

    def start(self):
        self.sock.bind((self.host, self.port))
        self.sock.listen(5)
        self.running = True
        logging.info(f"KVServer listening on {self.host}:{self.port}")
        
        try:
            while self.running:
                try:
                    self.sock.settimeout(1.0)  # Allow periodic checks
                    client, addr = self.sock.accept()
                    logging.info(f"Connection from {addr}")
                    threading.Thread(target=self.handle_client, args=(client, addr), daemon=True).start()
                except socket.timeout:
                    continue
                except Exception as e:
                    if self.running:
                        logging.error(f"Error accepting connection: {e}")
        finally:
            self.sock.close()
            logging.info("Server stopped")

    def stop(self):
        """Gracefully stop the server."""
        self.running = False

    def handle_client(self, client, addr):
        with client:
            try:
                # Receive message length first (4 bytes)
                length_data = self._recv_exact(client, 4)
                if not length_data:
                    return
                
                msg_length = struct.unpack('!I', length_data)[0]
                
                # Prevent excessive memory allocation
                if msg_length > 10 * 1024 * 1024:  # 10MB limit
                    logging.warning(f"Message too large from {addr}: {msg_length} bytes")
                    return
                
                # Receive the actual message
                data = self._recv_exact(client, msg_length)
                if not data:
                    return
                
                req = pickle.loads(data)
                
                # Validate request structure
                if not isinstance(req, dict) or 'cmd' not in req:
                    res = {'error': 'INVALID_REQUEST'}
                else:
                    res = self.process_command(req)
                
                # Send response with length prefix
                response_data = pickle.dumps(res)
                client.sendall(struct.pack('!I', len(response_data)))
                client.sendall(response_data)
                
            except (pickle.UnpicklingError, struct.error) as e:
                logging.error(f"Protocol error from {addr}: {e}")
                try:
                    error_response = pickle.dumps({'error': 'PROTOCOL_ERROR'})
                    client.sendall(struct.pack('!I', len(error_response)))
                    client.sendall(error_response)
                except:
                    pass
            except Exception as e:
                logging.error(f"Error handling client {addr}: {e}")
                try:
                    error_response = pickle.dumps({'error': str(e)})
                    client.sendall(struct.pack('!I', len(error_response)))
                    client.sendall(error_response)
                except:
                    pass

    def _recv_exact(self, sock, n):
        """Receive exactly n bytes from socket."""
        data = bytearray()
        while len(data) < n:
            packet = sock.recv(n - len(data))
            if not packet:
                return None
            data.extend(packet)
        return bytes(data)

    def process_command(self, req):
        """Process a command request."""
        cmd = req.get('cmd')
        
        try:
            if cmd == 'SET':
                if 'key' not in req or 'value' not in req:
                    return {'error': 'MISSING_PARAMETERS'}
                return self.kv.set(req['key'], req['value'])
            
            elif cmd == 'GET':
                if 'key' not in req:
                    return {'error': 'MISSING_PARAMETERS'}
                return self.kv.get(req['key'])
            
            elif cmd == 'DELETE':
                if 'key' not in req:
                    return {'error': 'MISSING_PARAMETERS'}
                return self.kv.delete(req['key'])
            
            elif cmd == 'BULK_SET':
                if 'items' not in req:
                    return {'error': 'MISSING_PARAMETERS'}
                if not isinstance(req['items'], list):
                    return {'error': 'INVALID_ITEMS_FORMAT'}
                return self.kv.bulk_set(req['items'])
            
            elif cmd == 'EXISTS':
                if 'key' not in req:
                    return {'error': 'MISSING_PARAMETERS'}
                return self.kv.exists(req['key'])
            
            elif cmd == 'KEYS':
                return self.kv.keys()
            
            else:
                return {'error': 'INVALID_CMD'}
                
        except Exception as e:
            logging.error(f"Error processing command {cmd}: {e}")
            return {'error': str(e)}

if __name__ == "__main__":
    server = KVServer('127.0.0.1', 65433, 'kvstore.db')
    try:
        server.start()
    except KeyboardInterrupt:
        logging.info("Shutting down server...")
        server.stop()