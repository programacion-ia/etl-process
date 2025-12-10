from pathlib import Path

import pandas as pd

from src.extractors import SqliteExtractor
from src.loaders import CsvLoader
from src.parsers import YamlParser
from src.utils import generate_output_path

YAML_FILE = 'config.yml'


def query_all_from_table(extractor: SqliteExtractor, table_name: str) -> pd.DataFrame:
    """Query all data from a given table in the SQLite database.

    Args:
        extractor (SqliteExtractor): The SQLite extractor instance.
        table_name (str): The name of the table to query.

    Returns:
        pd.DataFrame: DataFrame containing all data from the specified table.
    """
    query = f"SELECT * FROM {table_name};"
    return extractor.extract(query)

if __name__ == "__main__":
    yaml_parser = YamlParser()
    config = yaml_parser.load_yaml(YAML_FILE)
    
    db_extractor = SqliteExtractor('data/output/etl_output.db')
    
    continents_df = query_all_from_table(db_extractor, 'continents')

    countries_df = query_all_from_table(db_extractor, 'countries')

    db_path = generate_output_path(config, 'csv', 'continents')

    csv_loader = CsvLoader()
    csv_loader.load(continents_df, db_path)

    db_path = generate_output_path(config, 'csv', 'countries')
    csv_loader.load(countries_df, db_path)