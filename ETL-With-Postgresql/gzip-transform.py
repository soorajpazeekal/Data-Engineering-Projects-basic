import zipfile
import gzip
import shutil
import os
import configparser

config = configparser.ConfigParser(); config.read('config.ini')

def unzip_and_convert_to_gz(zip_file_path, target_file, output_gz_file):
    try:
        # Extract the zip file
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extract(target_file)

        # Convert the extracted file to gzip
        with open(target_file, 'rb') as file_in, gzip.open(output_gz_file, 'wb') as file_out:
            shutil.copyfileobj(file_in, file_out)

        print("Conversion successful. Output file:", output_gz_file); os.remove(zip_file_path); os.remove(target_file)

    except FileNotFoundError:
        print("Error: Zip file or target file not found.")

    except Exception as e:
        print("An error occurred:", str(e))


for item in os.listdir(config.get("DEFAULT", "DataFolderName")):
    if (item.endswith(".zip")):
        zip_file_path = f"{config.get('DEFAULT', 'DataFolderName')}/{item}"; csv_file_path = os.path.splitext(zip_file_path)[0]
        target_file = os.path.basename(csv_file_path);  output_gz_file = f"{csv_file_path}.gz"
        unzip_and_convert_to_gz(zip_file_path, target_file, output_gz_file)
        break

