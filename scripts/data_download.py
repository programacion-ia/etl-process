from pathlib import Path
import subprocess
import requests

from src.utils import ensure_data_directory
from src.parsers import parse_api_urls, YamlParser

YAML_FILE = Path.cwd() / 'config.yml'


if __name__=="__main__":
    yaml_parser = YamlParser()
    config = yaml_parser.load_yaml(YAML_FILE)
    
    data_dir = Path.cwd() / config['data_dir']['root_dir']
    ensure_data_directory(data_dir)
 
    api_urls = parse_api_urls(config)
    
    for name, (url, file_format) in api_urls.items():
        response = requests.get(url)
        data_dir = Path.cwd() / f"{config['data_dir']['root_dir']}/{config['data_dir'][name]['folder']}"
        file_path = data_dir / f"{config['data_dir'][name]['file']}.{file_format}"
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, 'wb') as file:
            file.write(response.content)
        if file_format == "zip":
            subprocess.run(['unzip', '-o', str(file_path), '-d', str(data_dir)])
            file_path.unlink()  # Remove the zip file after extraction

            # Change name of the generated file to dict key with subprocess (i know is only one file)
            extracted_files = list((data_dir).glob('*'))
            for extracted_file in extracted_files:
                if extracted_file.is_file() and extracted_file.name != f"{config['data_dir'][name]['file']}.zip":
                    extracted_file.rename(data_dir / f"{config['data_dir'][name]['file']}{extracted_file.suffix}")
