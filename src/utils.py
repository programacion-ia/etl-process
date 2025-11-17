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
    return Path.cwd() / \
            config['data_dir']['root_dir'] / \
            config['data_dir'][source]['folder'] / \
            f"{config['data_dir'][source]['file']}.{config['data_dir'][source]['extension']}"