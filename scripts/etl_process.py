from pathlib import Path

import pandas as pd

from src.parsers import YamlParser
from src.extractors import CsvExtractor, ExcelExtractor
from src.utils import generate_file_path, generate_output_path
from src.tranformers import EnergyTransformer, PopulationTransformer, EmissionsTransformer, PibTransformer, MergeTransformer, AggregateTransformer
from src.loaders import SqliteLoader

YAML_FILE = 'config.yml'

def extract(config: dict) -> tuple[pd.DataFrame]:
    """Extract data for all sources defined in the config.

    Args:
        config (dict): Configuration dictionary.
    """
    data_sources = config['data_sources']
    for source in data_sources:
        file_path = generate_file_path(config, source)
        
        match source:
            case 'global_emissions':
                csv_extractor = CsvExtractor()
                emissions_df = csv_extractor.extract(file_path)
            case 'population':
                csv_extractor = CsvExtractor()
                population_df = csv_extractor.extract(file_path)
            case 'pib':
                excel_extractor = ExcelExtractor(source)
                pib_df = excel_extractor.extract(file_path)
            case 'renewable_energy':
                excel_extractor = ExcelExtractor(source)
                energy_df = excel_extractor.extract(file_path)
        
    return emissions_df, pib_df, population_df, energy_df

def transform(emissions_df: pd.DataFrame, 
              pib_df: pd.DataFrame, 
              population_df: pd.DataFrame, 
              energy_df: pd.DataFrame
              ) -> list[pd.DataFrame]:
    """Transform input dataframes in order to obtain aggregated dataframes for countries and continents

    Args:
        emissions_df (pd.DataFrame): Global emissions data
        pib_df (pd.DataFrame): Pib per capita per country
        population_df (pd.DataFrame): Population stats
        energy_df (pd.DataFrame): Produced electricity per country

    Returns:
        list[pd.DataFrame]: Aggregated dataframes with mean values for countries and continents
    """
    population_tranformer = PopulationTransformer(population_df)
    population_df = population_tranformer.transform()

    energy_tranformer = EnergyTransformer(energy_df)
    energy_df = energy_tranformer.transform()

    emissions_transformer = EmissionsTransformer(emissions_df, population_df)
    emissions_df = emissions_transformer.transform()

    pib_transformer = PibTransformer(pib_df)
    pib_df = pib_transformer.transform()
    
    # Merge
    merge_tranformer = MergeTransformer(energy_df, emissions_df, pib_df, population_df)
    merged_df = merge_tranformer.transform()

    # Aggregated values
    aggregated_transformer = AggregateTransformer(merged_df)
    return aggregated_transformer.transform()

def load(config: dict,
         countries_df: pd.DataFrame, 
         continents_df: pd.DataFrame):
    db_path = generate_output_path(config, 'db')

    sqlite_loader = SqliteLoader(db_path)
    sqlite_loader.load(countries_df, 'countries')
    sqlite_loader.load(continents_df, 'continents')


if __name__ == "__main__":
    yaml_parser = YamlParser()
    config = yaml_parser.load_yaml(YAML_FILE)

    # Extract
    emissions_df, pib_df, population_df, energy_df = extract(config)

    # Transform
    countries_df, continents_df = transform(emissions_df, pib_df, population_df, energy_df)

    # Load
    load(config, countries_df, continents_df)