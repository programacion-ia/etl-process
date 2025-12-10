from pathlib import Path
import subprocess
import requests

from src.utils import ensure_data_directory
from src.parsers import YamlParser

YAML_FILE = Path.cwd() / 'config.yml'

def create_file(url, file_path):
    response = requests.get(url)
    file_path.parent.mkdir(parents=True, exist_ok=True)

    with open(file_path, 'wb') as file:
        file.write(response.content)

def unzip_file(file_path):
    subprocess.run(['unzip', '-o', str(file_path), '-d', str(file_path.parent)])
    file_path.unlink()  # Remove the zip file after extraction

def rename_extracted_file(file_path):
    # Change name of the generated file to dict key with subprocess (i know is only one file)
    extracted_files = list((file_path.parent).glob('*'))
    for extracted_file in extracted_files:
        if extracted_file.is_file() and extracted_file.stem != file_path.stem:
            extracted_file.rename(file_path.parent / f"{file_path.stem}{extracted_file.suffix}")

if __name__=="__main__":
    yaml_parser = YamlParser()
    config = yaml_parser.load_yaml(YAML_FILE)
    
    data_dir = Path(config['data_dir']['root_dir'])
    ensure_data_directory(data_dir)
 
    api_urls = {key: (value['url'], data_dir / value['path']) for key, value in config['download'].items()}
    
    for name, (url, file_path) in api_urls.items():
        create_file(url, file_path)
                
        if file_path.suffix == ".zip":
            unzip_file(file_path)
            rename_extracted_file(file_path)
