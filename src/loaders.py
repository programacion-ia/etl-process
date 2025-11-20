from abc import ABC, abstractmethod
from pathlib import Path

from sqlalchemy import create_engine
import pandas as pd


class BaseLoader(ABC):
    @abstractmethod
    def load(self):
        pass

class SqliteLoader(BaseLoader):
    def __init__(self, db_path: str | Path):
        self.engine = create_engine(f'sqlite:///{db_path}')

    def load(self, df: pd.DataFrame, table_name: str):
        df.to_sql(table_name, self.engine, index=False, if_exists='replace')