import pandas as pd
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class PopulationDataProcessor:
    def __init__(self, raw_data_path, processed_data_path):
        self.raw_data_path = raw_data_path
        self.processed_data_path = processed_data_path

    def load_data(self):
        """Load data from the raw CSV file."""
        try:
            df = pd.read_csv(self.raw_data_path)
            logging.info(f"Data loaded successfully from {self.raw_data_path}")
            return df
        except FileNotFoundError:
            logging.error(f"Error: The file {self.raw_data_path} does not exist.")
            return None
        except pd.errors.EmptyDataError:
            logging.error("Error: The file is empty.")
            return None
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            return None

    def clean_data(self, df):
        """Clean the data by removing commas, handling missing values, and standardizing column names."""
        if df is not None:
            # Remove commas from numbers and convert to integers
            for column in df.columns[1:]:
                df[column] = df[column].str.replace(',', '').astype(int)

            # Handle missing values
            df.fillna(0, inplace=True)

            # Validate data types
            for column in df.columns[1:]:
                if not pd.api.types.is_integer_dtype(df[column]):
                    logging.warning(f"Warning: Column {column} contains non-integer values.")

            # Remove duplicates
            df.drop_duplicates(inplace=True)

            # Standardize column names
            df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]

            logging.info("Data cleaned successfully")
            return df
        else:
            logging.error("Error: DataFrame is None, cannot clean data.")
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
            cleaned_df = self.clean_data(df)
            if cleaned_df is not None:
                self.save_data(cleaned_df)

if __name__ == "__main__":
    raw_data_path = '../raw/population_by_state.csv'
    processed_data_path = '../processed/population_by_state_cleaned.csv'
    
    processor = PopulationDataProcessor(raw_data_path, processed_data_path)
    processor.process()