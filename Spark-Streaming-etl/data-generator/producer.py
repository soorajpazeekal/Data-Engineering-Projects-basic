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
    """
    This function generates and returns JSON data for a fake order, including 
    order ID, name, email, total price, discount or promotion availability, 
    payment method, latitude, longitude, and creation timestamp.
    """
    location = json_data_from_file[f"location_{random.randint(1, 5)}"]
    data = {
    "Order_id": fake.uuid4(),
    "Name": fake.name(),
    "Email": fake.email(),
    "Total_Price": float(random.randint(1, 1000)),
    "Discount_or_Promotion": fake.random_element(elements=[True, False]),
    "PaymentMethod": fake.random_element(elements=["credit card", "cash"]),
    "latitude": float(location["lat"]),
    "longitude": location["lon"],
    "created_at": time.time()
    }
    json_data = json.dumps(data)
    time.sleep(random.randint(1, 30))
    return json_data