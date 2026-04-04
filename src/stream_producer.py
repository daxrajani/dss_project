import csv
import os
import time
import random
from datetime import datetime

def run_producer():
    print("\n>>> Starting DSS Live Data Producer...")
    
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    input_dir = os.path.join(base_dir, 'data', 'raw', 'stream_input')
    os.makedirs(input_dir, exist_ok=True)
    
    event_types = ['login', 'click', 'purchase', 'logout', 'error']
    batch_number = 1
    
    try:
        while True:
            # Generate a unique file for this micro-batch
            timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_name = f"batch_{batch_number}_{timestamp_str}.csv"
            file_path = os.path.join(input_dir, file_name)
            
            # Generate a random number of events for this batch (between 50 and 500)
            num_events = random.randint(50, 500)
            
            with open(file_path, mode='w', newline='') as file:
                writer = csv.writer(file)
                for _ in range(num_events):
                    user_id = random.randint(1, 1000)
                    event = random.choice(event_types)
                    timestamp = datetime.now().isoformat()
                    value = round(random.uniform(10.0, 500.0), 2) if event == 'purchase' else 0.0
                    
                    writer.writerow([user_id, event, timestamp, value])
            
            print(f">>> Dropped {file_name} ({num_events} events) into stream directory.")
            
            batch_number += 1
            # Wait 3 seconds before generating the next batch
            time.sleep(3)
            
    except KeyboardInterrupt:
        print("\n>>> Producer stopped by user.")

if __name__ == "__main__":
    run_producer()
