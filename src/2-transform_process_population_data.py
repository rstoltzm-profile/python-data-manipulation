import pandas as pd
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class PopulationDataProcessor:
    def __init__(self, cleaned_data_path, processed_data_path):
        self.cleaned_data_path = cleaned_data_path
        self.processed_data_path = processed_data_path

    def load_data(self):
        """Load data from the raw CSV file."""
        try:
            df = pd.read_csv(self.cleaned_data_path)
            logging.info(f"Data loaded successfully from {self.cleaned_data_path}")
            return df
        except FileNotFoundError:
            logging.error(f"Error: The file {self.cleaned_data_path} does not exist.")
            return None
        except pd.errors.EmptyDataError:
            logging.error("Error: The file is empty.")
            return None
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            return None

    def transform_data(self, df):
        """Transform the data to the desired row-column format and add US average population."""
        if df is not None:
            logging.info(f"Initial data shape: {df.shape}")
            df_melted = df.melt(id_vars=["label_(grouping)"], var_name="state", value_name="state_population")
            logging.info(f"Data shape after melting: {df_melted.shape}")
            
            # Check unique values in 'label_(grouping)' column
            unique_labels = df_melted["label_(grouping)"].unique()
            logging.info(f"Unique values in 'label_(grouping)': {unique_labels}")
            
            # Filter for the 'Total' row
            df_melted = df_melted[df_melted["label_(grouping)"].str.lower() == "total"]
            logging.info(f"Data shape after filtering: {df_melted.shape}")
            
            df_melted = df_melted[["state", "state_population"]]

            # Calculate US average population
            us_avg_population = df_melted["state_population"].mean()
            df_melted["us_avg_population"] = us_avg_population.round(0).astype(int)

            # Calculate the percentage difference from the US average population
            df_melted['PercentDiffFromUSAvg'] = (((df_melted['state_population'] - df_melted['us_avg_population']) / df_melted['us_avg_population']) * 100).round(2)

            # Create a new column 'Population_Rank' based on 'State_Population'
            df_melted['Population_Rank'] = df_melted['state_population'].rank(ascending=False).astype(int)

            # Sort the DataFrame based on 'Population_Rank'
            df_melted = df_melted.sort_values(by='Population_Rank')

            logging.info("Data transformed successfully")
            return df_melted
        else:
            logging.error("Error: DataFrame is None, cannot transform data.")
            return None

    def save_data(self, df):
        """Save the cleaned and transformed data to the processed CSV file."""
        if df is not None:
            try:
                df.to_csv(self.processed_data_path, index=False)
                logging.info(f"Data has been processed and saved to {self.processed_data_path}")
            except Exception as e:
                logging.error(f"An error occurred while saving the file: {e}")

    def process(self):
        """Load, clean, transform, and save the data."""
        df = self.load_data()
        if df is not None:
            transformed_df = self.transform_data(df)
            if transformed_df is not None:
                self.save_data(transformed_df)

if __name__ == "__main__":
    cleaned_data_path = '../processed/population_by_state_cleaned.csv'
    transformed_data_path = '../processed/population_by_state_transformed.csv'
    
    processor = PopulationDataProcessor(cleaned_data_path, transformed_data_path)
    processor.process()