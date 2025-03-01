import socket
import threading
import sys
import json
import time
from datetime import datetime

class Participant:
    def __init__(self, participant_id, log_file, coordinator_ip, coordinator_port):
        self.participant_id = participant_id
        self.log_file = log_file
        self.coordinator_ip = coordinator_ip
        self.coordinator_port = coordinator_port
        self.status = 'unregistered'  # 'unregistered', 'registered', 'disconnected'
        self.receiver_thread = None
        self.receiver_socket = None
        self.receiver_port = None
        self.stop_receiver = threading.Event()
    
    def start_command_interface(self):
        print(f"Participant {self.participant_id} started")
        print(f"Coordinator: {self.coordinator_ip}:{self.coordinator_port}")
        print("Available commands:")
        print("  register [port] - Register with the coordinator")
        print("  deregister - Deregister from the multicast group")
        print("  disconnect - Temporarily go offline")
        print("  reconnect [port] - Reconnect to the coordinator")
        print("  msend [message] - Send a multicast message")
        print("  exit - Exit the participant program")
        
        try:
            while True:
                command = input("> ").strip()
                if command.startswith("register "):
                    parts = command.split(" ", 1)
                    if len(parts) == 2:
                        try:
                            port = int(parts[1])
                            self.register(port)
                        except ValueError:
                            print("Invalid port number")
                elif command == "deregister":
                    self.deregister()
                elif command == "disconnect":
                    self.disconnect()
                elif command.startswith("reconnect "):
                    parts = command.split(" ", 1)
                    if len(parts) == 2:
                        try:
                            port = int(parts[1])
                            self.reconnect(port)
                        except ValueError:
                            print("Invalid port number")
                elif command.startswith("msend "):
                    parts = command.split(" ", 1)
                    if len(parts) == 2 and parts[1]:
                        self.msend(parts[1])
                    else:
                        print("Message cannot be empty")
                elif command == "exit":
                    if self.status == 'registered':
                        self.deregister()
                    break
                else:
                    print("Unknown command")
        except KeyboardInterrupt:
            print("\nExiting participant program...")
        finally:
            self.stop_receiver.set()
            if self.receiver_thread and self.receiver_thread.is_alive():
                self.receiver_thread.join(1)
    
    def start_message_receiver(self, port):
        self.stop_receiver.clear()
        self.receiver_port = port
        self.receiver_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.receiver_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        try:
            self.receiver_socket.bind(('', port))
            self.receiver_socket.listen(5)
            self.receiver_socket.settimeout(1)  # Set timeout for socket accept
            
            print(f"Message receiver started on port {port}")
            
            while not self.stop_receiver.is_set():
                try:
                    client_socket, address = self.receiver_socket.accept()
                    threading.Thread(target=self.handle_incoming_message, 
                                    args=(client_socket,), 
                                    daemon=True).start()
                except socket.timeout:
                    continue
                except Exception as e:
                    if not self.stop_receiver.is_set():
                        print(f"Error accepting connection: {e}")
                    break
        except Exception as e:
            print(f"Error starting message receiver: {e}")
        finally:
            if self.receiver_socket:
                self.receiver_socket.close()
                self.receiver_socket = None
            print("Message receiver stopped")
    
    def handle_incoming_message(self, client_socket):
        try:
            data = client_socket.recv(4096).decode('utf-8')
            if data:
                message_data = json.loads(data)
                if message_data['type'] == 'multicast':
                    sender_id = message_data['sender_id']
                    message = message_data['message']
                    timestamp = message_data['timestamp']
                    
                    # Log the message
                    timestamp_str = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
                    log_entry = f"[{timestamp_str}] From {sender_id}: {message}\n"
                    
                    with open(self.log_file, 'a') as f:
                        f.write(log_entry)
                    
                    print(f"Received multicast: [{timestamp_str}] From {sender_id}: {message}")
        except Exception as e:
            print(f"Error handling incoming message: {e}")
        finally:
            client_socket.close()
    
    def send_request_to_coordinator(self, request):
        try:
            # Create a socket for communicating with the coordinator
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect((self.coordinator_ip, self.coordinator_port))
            
            # Send the request
            client_socket.sendall(json.dumps(request).encode('utf-8'))
            
            # Wait for acknowledgement
            data = client_socket.recv(4096).decode('utf-8')
            response = json.loads(data)
            
            client_socket.close()
            
            if response.get('status') == 'ack':
                return True
            else:
                print("Coordinator did not acknowledge the request")
                return False
        except Exception as e:
            print(f"Error communicating with coordinator: {e}")
            return False
    
    def register(self, port):
        if self.status == 'registered':
            print("Already registered")
            return
        
        # Start the message receiver thread first
        self.receiver_thread = threading.Thread(target=self.start_message_receiver, args=(port,))
        self.receiver_thread.daemon = True
        self.receiver_thread.start()
        
        # Give the receiver thread time to start
        time.sleep(0.5)
        
        # Get local IP
        hostname = socket.gethostname()
        ip = socket.gethostbyname(hostname)
        
        # Send registration request
        request = {
            'command': 'register',
            'participant_id': self.participant_id,
            'ip': ip,
            'port': port
        }
        
        if self.send_request_to_coordinator(request):
            self.status = 'registered'
            print(f"Registered with coordinator using {ip}:{port}")
        else:
            self.stop_receiver.set()
            if self.receiver_thread and self.receiver_thread.is_alive():
                self.receiver_thread.join(1)
                self.receiver_thread = None
    
    def deregister(self):
        if self.status != 'registered':
            print("Not registered")
            return
        
        request = {
            'command': 'deregister',
            'participant_id': self.participant_id
        }
        
        if self.send_request_to_coordinator(request):
            self.status = 'unregistered'
            print("Deregistered from coordinator")
            
            # Stop the message receiver
            self.stop_receiver.set()
            if self.receiver_thread and self.receiver_thread.is_alive():
                self.receiver_thread.join(1)
                self.receiver_thread = None
    
    def disconnect(self):
        if self.status != 'registered':
            print("Not registered or already disconnected")
            return
        
        request = {
            'command': 'disconnect',
            'participant_id': self.participant_id
        }
        
        if self.send_request_to_coordinator(request):
            self.status = 'disconnected'
            print("Disconnected from coordinator")
            
            # Stop the message receiver
            self.stop_receiver.set()
            if self.receiver_thread and self.receiver_thread.is_alive():
                self.receiver_thread.join(1)
                self.receiver_thread = None
    
    def reconnect(self, port):
        if self.status != 'disconnected':
            print("Not in disconnected state")
            return
        
        # Start the message receiver thread first
        self.receiver_thread = threading.Thread(target=self.start_message_receiver, args=(port,))
        self.receiver_thread.daemon = True
        self.receiver_thread.start()
        
        # Give the receiver thread time to start
        time.sleep(0.5)
        
        # Get local IP
        hostname = socket.gethostname()
        ip = socket.gethostbyname(hostname)
        
        # Send reconnect request
        request = {
            'command': 'reconnect',
            'participant_id': self.participant_id,
            'ip': ip,
            'port': port
        }
        
        if self.send_request_to_coordinator(request):
            self.status = 'registered'
            print(f"Reconnected to coordinator using {ip}:{port}")
        else:
            self.stop_receiver.set()
            if self.receiver_thread and self.receiver_thread.is_alive():
                self.receiver_thread.join(1)
                self.receiver_thread = None
    
    def msend(self, message):
        if self.status != 'registered':
            print("Not registered with coordinator")
            return
        
        request = {
            'command': 'msend',
            'participant_id': self.participant_id,
            'message': message
        }
        
        if self.send_request_to_coordinator(request):
            print(f"Message sent: {message}")
        else:
            print("Failed to send message")

def main():
    if len(sys.argv) != 2:
        print("Usage: python participant.py <config_file>")
        sys.exit(1)
    
    config_file = sys.argv[1]
    
    try:
        with open(config_file, 'r') as f:
            participant_id = f.readline().strip()
            log_file = f.readline().strip()
            coordinator_info = f.readline().strip().split()
            coordinator_ip = coordinator_info[0]
            coordinator_port = int(coordinator_info[1])
    except Exception as e:
        print(f"Error reading config file: {e}")
        sys.exit(1)
    
    participant = Participant(participant_id, log_file, coordinator_ip, coordinator_port)
    participant.start_command_interface()

if __name__ == "__main__":
    main()