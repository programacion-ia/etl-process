from pathlib import Path

import pandas as pd

from src.parsers import YamlParser
from src.extractors import CsvExtractor, ExcelExtractor
from src.utils import generate_file_path

YAML_FILE = 'config.yml'

def create_source_variable(source: str, extractor: CsvExtractor | ExcelExtractor, file_path: str | Path) -> pd.DataFrame:
    """Dynamic creation of source dataframe from filepath.

    Args:
        source (str): Source name
        extractor (CsvExtractor | ExcelExtractor): Extractor to be used
        file_path (str | Path): Path to file to be loaded.
    """
    vars().update({f"{source}_df": extractor.extract(file_path)})
    return locals()[f"{source}_df"]

def extract(config: dict) -> tuple[pd.DataFrame]:
    """Extract data for all sources defined in the config.

    Args:
        config (dict): Configuration dictionary.
    """
    data_sources = config['data_sources']
    csv_extractor = CsvExtractor()
    dataframes = {}
    for source in data_sources:
        data_info = config['data_dir'][source]
        file_path = generate_file_path(config, source)
        
        if data_info['extension'] == 'csv':
            dataframes[source] = create_source_variable(source, csv_extractor, file_path)
        
        elif data_info['extension'] in ['xls', 'xlsx']:
            excel_extractor = ExcelExtractor(source)
            dataframes[source] = create_source_variable(source, excel_extractor, file_path)
    
    return dataframes['global_emissions'], dataframes['pib'], dataframes['population'], dataframes['renewable_energy']


if __name__ == "__main__":
    yaml_parser = YamlParser()
    config = yaml_parser.load_yaml(YAML_FILE)

    # Extract
    emissions_df, pib_df, population_df, energy_df = extract(config)

    a=0