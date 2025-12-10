from pathlib import Path


def ensure_data_directory(data_dir: Path) -> None:
    """Ensure that the data directory exists

    Args:
        data_dir (Path): Path to the data directory
    """
    if not data_dir.exists():
        data_dir.mkdir(parents=True)

def generate_file_path(config: dict, source: str) -> Path:
    """Generate the full file path for a given data source based on the configuration.

    Args:
        config (dict): Configuration dictionary containing data directory info.
        source (str): The key for the data source.

    Returns:
        Path: Full path to the data file.
    """
    return  Path(config['data_dir']['root_dir']) / \
            config['data_dir'][source]['folder'] / \
            f"{config['data_dir'][source]['file']}.{config['data_dir'][source]['extension']}"

def generate_output_path(config: dict, output_type: str, csv_file: str = None) -> Path:
    """Generate the full output file path based on the configuration.

    Args:
        config (dict): Configuration dictionary containing data directory info.
        filename (str): The name of the output file.
        csv_file (str, optional): Specific CSV file name if output_type is 'csv'. Defaults to None.

    Returns:
        Path: Full path to the output data file.
    """
    db_root_path = Path.cwd() / config['data_dir']['root_dir'] /config['data_dir']['outputs']['root']
    ensure_data_directory(db_root_path)
    
    if output_type == 'db':
        return db_root_path / config['data_dir']['outputs']['database']
    elif output_type == 'csv' and csv_file:
        return db_root_path / config['data_dir']['outputs']['csv'][csv_file]