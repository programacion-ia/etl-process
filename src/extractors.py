from abc import ABC, abstractmethod
from pathlib import Path

import pandas as pd


class BaseExtractor(ABC):
    @abstractmethod
    def extract(self, data):
        pass

class ExcelExtractor(BaseExtractor):
    """Extracts data from Excel files."""

    def __init__(self, name: str):
        self.name = name

    def extract(self, file_path: str | Path) -> pd.DataFrame:
        """Extract data from filepath

        Args:
            file_path (str | Path): Path to Excel file

        Returns:
            pd.DataFrame: File DataFrame
        """
        if self.name == 'pib':
            return pd.read_excel(file_path, sheet_name='Full data')
        elif self.name == 'renewable_energy':
            return pd.read_excel(file_path, sheet_name='Data', header=3)
        else:
            return pd.read_excel(file_path)

class CsvExtractor(BaseExtractor):
    """Extracts data from CSV files."""

    def extract(self, file_path: str | Path) -> pd.DataFrame:
        """Extract data from a filepath

        Args:
            file_path (str | Path): Path to csv file

        Returns:
            pd.DataFrame: File DataFrame
        """
        return pd.read_csv(file_path)