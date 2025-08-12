import pandas as pd
from rich import print
import yaml
from typing import Literal


class DataPreprocessor:
    def __init__(self, config_path, file_paths: list):
        with open(config_path, "r") as file:
            self.config = yaml.safe_load(file)
        self.file_paths = file_paths

    def _remove_redundant_columns(self, df: pd.DataFrame, source: Literal["glassdoor", "indeed", "linkedIn"]) -> pd.DataFrame:
        return df.drop(self.config[source].get('columns_to_remove', []), axis=1, errors="ignore")


    def _merge_columns(self, df: pd.DataFrame, source: Literal["glassdoor", "indeed", "linkedIn"]) -> pd.DataFrame:
        merge_column_names = self.config[source].get("columns_to_merge")
        # incase there is not columns to rename
        if not merge_column_names:
            print("No columns to rename, skipping to next step ...")
            return df

        # Keep track of all columns to drop after merging
        cols_to_drop = set()

        for new_col, cols in merge_column_names.items():
            # Build dict-like string for each row
            def row_to_dict(row):
                non_nulls = {col: row[col] for col in cols if pd.notna(row[col])}
                return str(non_nulls)

            df[new_col] = df.apply(row_to_dict, axis=1)
            cols_to_drop.update(cols)

        # Drop the original columns used for merging
        df = df.drop(columns=list(cols_to_drop), errors="ignore")
        return df


    def _rename_columns(self, df: pd.DataFrame, source: Literal["glassdoor", "indeed", "linkedIn"]) -> pd.DataFrame:
        rename_mapping = self.config[source].get("columns_to_rename")
        if rename_mapping:
            df.rename(columns=rename_mapping, inplace=True)
        return df


    def _sort_columns(self, df: pd.DataFrame, source: Literal["glassdoor", "indeed", "linkedIn"]) -> pd.DataFrame:
        new_column_order = self.config[source].get("column_sort_order")
        df_custom_sorted = df[new_column_order]
        return df_custom_sorted

    def runner(self, output_path):
        if len(self.file_paths) == 0:
            return False
        datasets = []
        for url in self.file_paths:
            if "linkedin" in url:
                source_ = "linkedIn"
            elif "indeed" in url:
                source_ = "indeed"
            elif "glassdoor" in url:
                source_ = "glassdoor"
            else:
                raise f"{url} is not from allowed source or not correctly named."

            # Processing files
            data = pd.read_csv(url)
            data_sample = self._remove_redundant_columns(data, source=source_)
            output = self._merge_columns(data_sample, source=source_)
            new_output = self._rename_columns(df=output, source=source_)
            sorted_columns = self._sort_columns(new_output, source=source_)
            datasets.append(sorted_columns)

        # Merging the columns into 1
        concatenated_df = pd.concat(datasets, ignore_index=True)

        # Removing duplicated rows.
        before = len(concatenated_df)

        # Removing duplicated rows.
        concatenated_df.drop_duplicates(inplace=True)
        removed = before - len(concatenated_df)

        concatenated_df.to_csv(output_path, index=False)
        print(f"✅ Data Cleaned successfully and saved to {output_path} — {removed} duplicates removed")
        return True
