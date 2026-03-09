import csv
import os
import random
from datetime import datetime, timedelta

def generate_data(num_records, output_file):
    # This ensures the file saves to data/raw no matter where the script is run from
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    file_path = os.path.join(base_dir, 'data', 'raw', output_file)

    event_types = ['login', 'click', 'purchase', 'logout', 'error']
    
    print(f"Generating {num_records} records into {file_path}...")
    
    with open(file_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['user_id', 'event_type', 'timestamp', 'value'])
        
        base_time = datetime.now()
        
        for i in range(num_records):
            user_id = random.randint(1, 10000)
            event = random.choice(event_types)
            timestamp = (base_time + timedelta(seconds=i)).isoformat()
            value = round(random.uniform(10.0, 500.0), 2) if event == 'purchase' else 0.0
            
            writer.writerow([user_id, event, timestamp, value])
            
    print("Generation complete!")

if __name__ == "__main__":
    generate_data(2000000, 'massive_events.csv')
