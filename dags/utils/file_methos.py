import os
import json

def write_file(conf_files, json_data):
    target_dir, file_name = conf_files
    if not os.path.isdir(target_dir):
        os.makedirs(target_dir)
    with open(f"{target_dir}/{file_name}", "w") as data:
        json.dump(json_data, data)

def remove_temp_files(target_directory):
    for f in os.listdir(target_directory):
        os.remove(os.path.join(target_directory, f))