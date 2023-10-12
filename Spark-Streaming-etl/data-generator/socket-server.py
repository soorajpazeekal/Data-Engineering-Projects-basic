import socket
import time
from producer import need_data


server_address = ('localhost', 8888)  


server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

server_socket.bind(server_address)
server_socket.listen(5)
print("Server is listening for incoming connections...")

while True:
    client_socket, client_address = server_socket.accept()
    print(f"Accepted connection from {client_address}")

    try:
        while True:
            print("sending data to client...")
            final_data = need_data()
            data_to_send = final_data+"\n"
            client_socket.send(data_to_send.encode('utf-8'))
    except KeyboardInterrupt:
        print("Server is shutting down...")
        break
    except Exception as e:
        print(f"Error: {e}")
    finally:

        client_socket.close()


server_socket.close()
