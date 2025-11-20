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
        columns = ['CCA3', 'Country/Territory', 'Continent', '2010 Population']
        self.df = self.df[columns]
        self.df.rename(columns={'CCA3': 'Country Code','Country/Territory': 'Country Name', '2010 Population': 'Population'}, inplace=True)

class EnergyTransformer(BaseTransformer):
    
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def transform(self) -> pd.DataFrame:
        self.__select_columns()
        self.__transpose_df()
        # self.__add_population_column()
        self.__delete_na_rows()
        self.__cast_year_to_int()
        # self.__calculate_per_capita_energy()
        # self.__calculate_mean_parameters()
        return self.df
    
    def __select_columns(self) -> pd.DataFrame:
        columns = self.__generate_columns()
        self.df = self.df[columns]
    
    @staticmethod
    def __generate_columns(init: int = 1990, end: int = 2014) -> list[str]:
        dates = [str(year) for year in range(init, end + 1)]
        columns = ['Country Code', 'Country Name'] + dates
        return columns
    
    def __transpose_df(self):
        self.df = self.df.melt(id_vars=['Country Code', 'Country Name'], var_name='Year', value_name='Energy')

    def __delete_na_rows(self):
        self.df = self.df.dropna(subset=['Energy'])
    
    def __add_population_column(self):
        merged_df = pd.merge(self.df, self.population_df, how='left', left_on=['Country Code'], right_on=['Country Code'])
        self.df = merged_df
    
    def __calculate_per_capita_energy(self):
        self.df['Energy per Capita'] = self.df['Energy'] / self.df['Population']
        self.df = self.df.drop(columns=['Population'])
    
    def __calculate_mean_parameters(self):
        self.df = self.df.groupby(['Country Code', 'Country Name'], as_index=False).agg({'Energy': 'mean', 'Energy per Capita': 'mean'})

    def __cast_year_to_int(self):
        self.df['Year'] = self.df['Year'].astype(int)
class EmissionsTransformer(BaseTransformer):
    def __init__(self, df: pd.DataFrame, population_df: pd.DataFrame):
        self.df: pd.DataFrame = df
        self.population_df = population_df

    def transform(self) -> pd.DataFrame:
        self.__pivot_df()
        self.__drop_na_columns()
        self.__add_country_codes()
        self.__rename_columns()
        return self.df
    
    def __pivot_df(self):
        self.df = self.df.pivot(columns='category', values='value', index=['country_or_area', 'year']).reset_index()
        
    def __drop_na_columns(self, na_threshold: int = 200):
        na_counts = self.df.isna().sum()
        cols_to_drop = na_counts[na_counts > na_threshold].index.tolist()
        if cols_to_drop:
            self.df = self.df.drop(columns=cols_to_drop)

    def __add_country_codes(self):
        columns = ['Country Code', 'Country Name'] + self.df.columns.tolist()
        self.df = pd.merge(self.df, self.population_df[['Country Code', 'Country Name']], how='left', left_on=['country_or_area'], right_on=['Country Name'])
        self.__order_columns(columns)

    def __order_columns(self, columns: list[str]):
        self.df = self.df[columns]
        self.df.drop(labels='country_or_area', axis=1, inplace=True)

    def __rename_columns(self):
        self.df.rename(columns={'year': 'Year'}, inplace=True)

class PibTransformer(BaseTransformer):
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def transform(self) -> pd.DataFrame:
        self.__filter_columns()
        self.__filter_dates()
        self.__rename_columns()
        return self.df
    
    def __filter_columns(self):
        self.df.drop(labels='pop', axis=1, inplace=True)

    def __filter_dates(self, start_year: int = 1990, end_year: int = 2014):
        self.df = self.df[(self.df['year'] >= start_year) & (self.df['year'] <= end_year)]

    def __rename_columns(self):
        self.df.rename(columns={'countrycode': 'Country Code', 'country':  'Country Name', 'year': 'Year', 'gdppc': 'pib'}, inplace=True)


class MergeTransformer(BaseTransformer):
    def __init__(self, energy_df: pd.DataFrame, emissions_df: pd.DataFrame, pib_df: pd.DataFrame, population_df: pd.DataFrame):
        self.energy_df = energy_df
        self.emissions_df = emissions_df
        self.pib_df = pib_df
        self.population_df = population_df

    def transform(self) -> pd.DataFrame:
        self.__merge_energy_emissions()
        self.__merge_pib()
        self.__merge_population()
        self.__order_columns()
        return self.merged_df
    
    def __merge_energy_emissions(self):
        self.merged_df = pd.merge(self.energy_df, self.emissions_df, how='inner', left_on=['Country Code'], right_on=['Country Code'], suffixes=(None, '_y'))
        self.__delete_redundant_columns()

    def __delete_redundant_columns(self):
        cols_to_drop = [c for c in self.merged_df.columns if isinstance(c, str) and c.endswith('_y')]
        if cols_to_drop:
            self.merged_df.drop(columns=cols_to_drop, inplace=True)

    def __merge_pib(self):
        self.merged_df = pd.merge(self.merged_df, self.pib_df, how='inner', left_on=['Country Code'], right_on=['Country Code'], suffixes=(None, '_y'))
        self.__delete_redundant_columns()

    def __merge_population(self):
        self.merged_df = pd.merge(self.merged_df, self.population_df, how='inner', left_on=['Country Code'], right_on=['Country Code'], suffixes=(None, '_y'))
        self.__delete_redundant_columns()

    def __order_columns(self, 
                        first_columns: list[str] = ['Country Code', 
                                                    'Country Name', 
                                                    'Continent', 
                                                    'Year', 
                                                    'Population', 
                                                    'pib']):
        ordered_columns = [c for c in first_columns if c in self.merged_df.columns] + [c for c in self.merged_df.columns if c not in first_columns]
        self.merged_df = self.merged_df[ordered_columns]