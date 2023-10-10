from faker import Faker
from faker.providers import geo
import json
import random
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
    time.sleep(random.randint(1, 5))
    return json_data