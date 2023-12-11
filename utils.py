import pandas as pd
import os
from pathlib import Path

OUTPUT_DIR_PATH: Path = Path(os.path.join(os.getcwd(), "VWAP_output_hourly"))
OUTPUT_DIR_PATH.mkdir(parents=True, exist_ok=True)

def output_df_as_csv(df: pd.DataFrame, file_name: str)->None:
    """
    Output DataFrame to a CSV file with the specified file name.

    Args:
    df (pd.DataFrame): DataFrame to be saved.
    file_name (str): CSV file's name.
    """
    output_file_path = OUTPUT_DIR_PATH / f"{file_name}.csv"
    df.to_csv(output_file_path, index=False)