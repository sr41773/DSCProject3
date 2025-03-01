import socket
import threading
import sys
import time
import json
from datetime import datetime

class Coordinator:
    def __init__(self, port, persistence_time):
        self.port = port
        self.persistence_time = persistence_time  # in seconds
        self.members = {}  # {participant_id: {'ip': ip, 'port': port, 'status': 'online'/'offline'}}
        self.messages = []  # [{sender_id, message, timestamp}]
        self.lock = threading.Lock()
        
    def start(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind(('', self.port))
        server_socket.listen(10)
        
        print(f"Coordinator started on port {self.port}")
        print(f"Persistence time threshold: {self.persistence_time} seconds")
        
        try:
            while True:
                client_socket, address = server_socket.accept()
                client_handler = threading.Thread(target=self.handle_client, args=(client_socket, address))
                client_handler.daemon = True
                client_handler.start()
        except KeyboardInterrupt:
            print("Coordinator shutting down...")
        finally:
            server_socket.close()
    
    def handle_client(self, client_socket, address):
        try:
            data = client_socket.recv(4096).decode('utf-8')
            if data:
                # Parse request
                request = json.loads(data)
                
                # Acknowledge receipt
                ack = {'status': 'ack'}
                client_socket.sendall(json.dumps(ack).encode('utf-8'))
                
                # Process request
                self.process_request(request, address)
        except Exception as e:
            print(f"Error handling client: {e}")
        finally:
            client_socket.close()
    
    def process_request(self, request, address):
        command = request.get('command')
        participant_id = request.get('participant_id')
        
        if command == 'register':
            self.register_participant(participant_id, request.get('ip'), request.get('port'))
        elif command == 'deregister':
            self.deregister_participant(participant_id)
        elif command == 'disconnect':
            self.disconnect_participant(participant_id)
        elif command == 'reconnect':
            self.reconnect_participant(participant_id, request.get('ip'), request.get('port'))
        elif command == 'msend':
            self.handle_multicast(participant_id, request.get('message'))
    
    def register_participant(self, participant_id, ip, port):
        with self.lock:
            self.members[participant_id] = {
                'ip': ip,
                'port': port,
                'status': 'online',
                'last_disconnect_time': None
            }
            print(f"Participant {participant_id} registered from {ip}:{port}")
    
    def deregister_participant(self, participant_id):
        with self.lock:
            if participant_id in self.members:
                del self.members[participant_id]
                print(f"Participant {participant_id} deregistered")
    
    def disconnect_participant(self, participant_id):
        with self.lock:
            if participant_id in self.members:
                self.members[participant_id]['status'] = 'offline'
                self.members[participant_id]['last_disconnect_time'] = time.time()
                print(f"Participant {participant_id} disconnected")
    
    def reconnect_participant(self, participant_id, ip, port):
        with self.lock:
            if participant_id in self.members:
                self.members[participant_id]['ip'] = ip
                self.members[participant_id]['port'] = port
                self.members[participant_id]['status'] = 'online'
                
                # Send missed messages within persistence time threshold
                disconnect_time = self.members[participant_id]['last_disconnect_time']
                current_time = time.time()
                
                # If participant was disconnected for less than persistence_time
                # or if disconnect_time is None (first connection), send relevant messages
                if disconnect_time is not None and current_time - disconnect_time <= self.persistence_time:
                    self.send_missed_messages(participant_id)
                
                print(f"Participant {participant_id} reconnected from {ip}:{port}")
    
    def handle_multicast(self, sender_id, message):
        current_time = time.time()
        
        # Store message for persistence
        with self.lock:
            message_entry = {
                'sender_id': sender_id,
                'message': message,
                'timestamp': current_time
            }
            self.messages.append(message_entry)
            
            # Send message to all online participants
            for participant_id, participant_info in self.members.items():
                if participant_info['status'] == 'online':
                    self.send_message_to_participant(participant_id, message_entry)
            
            print(f"Multicast message from {sender_id}: {message}")
    
    def send_missed_messages(self, participant_id):
        if participant_id not in self.members:
            return
        
        disconnect_time = self.members[participant_id]['last_disconnect_time']
        current_time = time.time()
        cutoff_time = current_time - self.persistence_time
        
        # Use the later of disconnect_time or cutoff_time
        effective_cutoff = max(disconnect_time, cutoff_time) if disconnect_time is not None else cutoff_time
        
        for message_entry in self.messages:
            # Send only messages that were sent after the participant disconnected
            # and within the persistence time threshold
            if (message_entry['timestamp'] >= effective_cutoff and 
                (disconnect_time is None or message_entry['timestamp'] >= disconnect_time)):
                self.send_message_to_participant(participant_id, message_entry)
    
    def send_message_to_participant(self, participant_id, message_entry):
        if participant_id not in self.members:
            return
            
        participant_info = self.members[participant_id]
        if participant_info['status'] != 'online':
            return
            
        try:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect((participant_info['ip'], participant_info['port']))
            
            payload = {
                'type': 'multicast',
                'sender_id': message_entry['sender_id'],
                'message': message_entry['message'],
                'timestamp': message_entry['timestamp']
            }
            
            client_socket.sendall(json.dumps(payload).encode('utf-8'))
            client_socket.close()
        except Exception as e:
            print(f"Error sending message to participant {participant_id}: {e}")
            # Mark participant as offline since we couldn't reach them
            with self.lock:
                self.members[participant_id]['status'] = 'offline'
                self.members[participant_id]['last_disconnect_time'] = time.time()

def main():
    if len(sys.argv) != 2:
        print("Usage: python coordinator.py <config_file>")
        sys.exit(1)
    
    config_file = sys.argv[1]
    
    try:
        with open(config_file, 'r') as f:
            port = int(f.readline().strip())
            persistence_time = int(f.readline().strip())
    except Exception as e:
        print(f"Error reading config file: {e}")
        sys.exit(1)
    
    coordinator = Coordinator(port, persistence_time)
    coordinator.start()

if __name__ == "__main__":
    main()