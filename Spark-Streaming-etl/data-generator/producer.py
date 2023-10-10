from faker import Faker
from faker.providers import geo
import json
import random
import socket
import time


fake = Faker()
fake.add_provider(geo)
with open('sams-club-loc.json', 'r') as json_file:
    json_data_from_file = json.load(json_file)



def need_data():
    data = {
    "Order_id": fake.uuid4(),
    "Name": fake.name(),
    "Address": fake.address(),
    "Location": json_data_from_file[f"location_{random.randint(1, 5)}"],
    "created_at": time.time()
    }
    json_data = json.dumps(data)
    print(json_data)
    return json_data


def write_to_socket():
    while True:
        print("Sending data to socket...")
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_address = ('127.0.0.1', 36768)
        client_socket.connect(server_address)
        client_socket.send("hi".encode())
        client_socket.close()
        time.sleep(random.randint(1, 3))


write_to_socket()