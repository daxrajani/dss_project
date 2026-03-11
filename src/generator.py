import csv
import os
import random
import uuid
from datetime import datetime, timedelta

def generate_ecommerce_data():
    print("\n>>> Generating E-commerce Datasets (Catalog, Events, Orders)...")
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    raw_dir = os.path.join(base_dir, 'data', 'raw')
    os.makedirs(raw_dir, exist_ok=True)

    # 1. Generate Catalog
    products = []
    categories = ['Electronics', 'Clothing', 'Home', 'Books']
    brands = ['BrandA', 'BrandB', 'BrandC', 'Generic']
    
    with open(os.path.join(raw_dir, 'catalog.csv'), 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['product_id', 'category', 'brand', 'price'])
        for pid in range(1, 101):
            price = round(random.uniform(5.0, 1500.0), 2)
            products.append((pid, price))
            writer.writerow([pid, random.choice(categories), random.choice(brands), price])

    # 2. Generate Events and Orders
    users = range(1, 501) # 500 users
    devices = ['mobile', 'desktop', 'tablet']
    referrers = ['google', 'facebook', 'direct', 'email']
    event_types = ['view', 'add_to_cart', 'purchase']
    
    events_file = open(os.path.join(raw_dir, 'events.csv'), 'w', newline='')
    orders_file = open(os.path.join(raw_dir, 'orders.csv'), 'w', newline='')
    
    events_writer = csv.writer(events_file)
    orders_writer = csv.writer(orders_file)
    
    events_writer.writerow(['user_id', 'timestamp', 'event_type', 'product_id', 'device', 'referrer'])
    orders_writer.writerow(['order_id', 'user_id', 'timestamp', 'total_amount'])

    base_time = datetime(2026, 3, 1) # Starting March 1st

    # Generate logical user journeys
    for user in users:
        current_time = base_time + timedelta(days=random.randint(0, 10), hours=random.randint(0, 23))
        
        # Some users have multiple sessions (gaps > 30 mins)
        for session_loop in range(random.randint(1, 3)):
            device = random.choice(devices)
            referrer = random.choice(referrers)
            
            # 1 to 10 events per session
            for _ in range(random.randint(1, 10)):
                event = random.choices(event_types, weights=[70, 20, 10])[0]
                prod_id, price = random.choice(products)
                
                events_writer.writerow([user, current_time.isoformat(), event, prod_id, device, referrer])
                
                if event == 'purchase':
                    order_id = str(uuid.uuid4())[:8]
                    orders_writer.writerow([order_id, user, current_time.isoformat(), price])
                
                # Increment time by a few seconds/minutes for the next event
                current_time += timedelta(seconds=random.randint(10, 300))
            
            # Jump forward in time by at least 1 hour to force a NEW session next loop
            current_time += timedelta(hours=random.randint(1, 48))

    events_file.close()
    orders_file.close()
    print(">>> Generation Complete! Saved to data/raw/")

if __name__ == "__main__":
    generate_ecommerce_data()
