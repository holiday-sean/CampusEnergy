import pandas as pd
import numpy as np
from pathlib import Path


class SkySparkCleaner():
    def __init__(self, raw_dir="data/raw", clean_dir="data/processed"):
        self.raw_dir = Path(raw_dir)
        self.clean_dir = Path(clean_dir)

    def remove_duplicate_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove columns that contain digits, which are assumed to be duplicates.
        """
        dupe_cols = [col for col in df.columns if any(c.isdigit() for c in col)]
        return df.drop(columns=dupe_cols)

    def split_timestamp(self, df: pd.DataFrame) -> pd.DataFrame:
        ts_parsed = pd.to_datetime(
            df["ts"].str.split(" ").str[0].str.replace("Z", "", regex=False),
            format="%Y-%m-%dT%H:%M:%S",
            errors="coerce"
        )

        df["Date"] = ts_parsed.dt.strftime("%Y-%m-%d")
        df["Time"] = ts_parsed.dt.strftime("%H:%M:%S")

        return df

    def convert_building_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert building-related columns to float after removing invalid characters.
        """
        ignore = {"ts", "Date", "Time"}
        building_cols = [c for c in df.columns if c not in ignore]

        df[building_cols] = (
            df[building_cols]
            .replace(r"[^\dEe+\-.]", "", regex=True)
            .replace("", np.nan)
            .astype(float)
        )
        return df

    # -----------------------------
    # Full Cleaning Pipeline
    # -----------------------------
    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Full end-to-end cleaning pipeline.
        """
        df = self.remove_duplicate_columns(df)
        df = self.split_timestamp(df)
        df = self.convert_building_columns(df)
        return df

    # -----------------------------
    # File Handling
    # -----------------------------
    def get_all_raw_files(self):
        """
        Collect file paths from all raw data subdirectories.
        """
        return list(self.raw_dir.rglob("*.csv"))

    def clean_file(self, filepath: Path):
        """
        Clean a single CSV and save to clean directory, preserving filename.
        """
        df = pd.read_csv(filepath, low_memory=False)
        clean_df = self.clean(df)

        relative_path = filepath.relative_to(self.raw_dir)
        output_path = self.clean_dir / relative_path

        output_path.parent.mkdir(parents=True, exist_ok=True)
        clean_df.to_csv(output_path, index=False)

    def run(self):
        """
        Clean all raw CSV files and export them to processed directory.
        """
        for f in self.get_all_raw_files():
            self.clean_file(f)


if __name__ == "__main__":
    cleaner = SkySparkCleaner()
    cleaner.run()
