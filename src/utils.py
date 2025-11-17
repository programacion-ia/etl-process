from pathlib import Path


def ensure_data_directory(data_dir: Path) -> None:
    """Ensure that the data directory exists

    Args:
        data_dir (Path): Path to the data directory
    """
    if not data_dir.exists():
        data_dir.mkdir(parents=True)