import re
import os
import csv
import hl7apy as hl7

# File paths
log_file = "./logfile"
input_dir = "./input"
output_csv = "./output/results.csv"

def parse_log_file(log_file):
    error_map = {}
    pattern = r"'(sfh_adt|nuh_adt)'(\d{20})'"
    
    with open(log_file, 'r') as file:
        for line in file:
            match = re.search(pattern, line)
            if match:
                timestamp = match.group(2)
                if timestamp in error_map:
                    error_map[timestamp] += " | " + line.strip()
                else:
                    error_map[timestamp] = line.strip()
    
    return error_map

def process_hl7_files(input_dir, error_map, output_csv):
    os.makedirs(os.path.dirname(output_csv), exist_ok=True)
    
    with open(output_csv, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Filename", "PID", "Error", "Action (Keep/Discard)"])
        
        for filename in os.listdir(input_dir):
            file_path = os.path.join(input_dir, filename)
            
            try:
                with open(file_path, 'r') as file:
                    message = hl7.parser.parse_message(file.read(), find_groups=False)
                    
                    # Extract relevant fields
                    msh7_timestamp = message.segment("MSH")[7][0] if message.segment("MSH")[7] else ""
                    pid_segment = str(message.segment("PID")) if message.segment("PID") else ""
                    
                    # Match timestamp to error message
                    error_message = error_map.get(msh7_timestamp, "No error found")
                    
                    # Write to CSV
                    writer.writerow([filename, pid_segment, error_message, ""])
            except Exception as e:
                print(f"Error processing {filename}: {e}")

# Execute processing
error_map = parse_log_file(log_file)
process_hl7_files(input_dir, error_map, output_csv)
