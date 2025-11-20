from abc import ABC, abstractmethod

import pandas as pd

class BaseTransformer(ABC):
    @abstractmethod
    def transform(self, data):
        pass

class PopulationTransformer(BaseTransformer):
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def transform(self):
        self.select_columns()
        return self.df
    
    def select_columns(self):
        columns = ['CCA3', '2010 Population']
        self.df = self.df[columns]
        self.df.rename(columns={'CCA3': 'Country Code', '2010 Population': 'Population'}, inplace=True)

class EnergyTransformer(BaseTransformer):
    
    def __init__(self, df: pd.DataFrame, population_df: pd.DataFrame):
        self.df = df
        self.population_df = population_df

    def transform(self):
        self.__select_columns()
        self.__transpose_df()
        # self.__add_population_column()
        self.__delete_na_rows()
        # self.__calculate_per_capita_energy()
        # self.__calculate_mean_parameters()
        return self.df
    
    def __select_columns(self) -> pd.DataFrame:
        columns = self.__generate_columns()
        self.df = self.df[columns]
    
    @staticmethod
    def __generate_columns():
        init = 1990
        end = 2014
        dates = [str(year) for year in range(init, end + 1)]
        columns = ['Country Name', 'Country Code'] + dates
        return columns
    
    def __transpose_df(self):
        self.df = self.df.melt(id_vars=['Country Name', 'Country Code'], var_name='Year', value_name='Energy')

    def __delete_na_rows(self):
        self.df = self.df.dropna(subset=['Energy', 'Population'])
    
    def __add_population_column(self):
        merged_df = pd.merge(self.df, self.population_df, how='left', left_on=['Country Code'], right_on=['Country Code'])
        self.df = merged_df
    
    def __calculate_per_capita_energy(self):
        self.df['Energy per Capita'] = self.df['Energy'] / self.df['Population']
        self.df = self.df.drop(columns=['Population'])
    
    def __calculate_mean_parameters(self):
        self.df = self.df.groupby(['Country Code', 'Country Name'], as_index=False).agg({'Energy': 'mean', 'Energy per Capita': 'mean'})