from pathlib import Path

import yaml


class YamlParser:

    @staticmethod
    def load_yaml(file_path: str | Path) -> dict:
        """Load YML file

        Args:
            file_path (str | Path): Path to the YAML file

        Returns:
            dict: YAML content
        """
        with open(file_path, 'r') as file:
            return yaml.safe_load(file)
    
def parse_api_urls(config: dict) -> dict:
    """Prepare urls for downloading

    Args:
        config (dict): Configuration in YAML file

    Returns:
        dict: Prepared dict
    """
    return {values['name']: [values['url'], values['extension']] for item in config['api_urls'] for key, values in item.items()}