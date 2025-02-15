import json
from pathlib import Path
import locale
from tabulate import tabulate

import pandas as pd


def sanitize_column_name(name: str):
    if isinstance(name, tuple):  # Handle tuple column names
        new_name = tuple(
            str(x)
            .replace("@", "")
            .replace("#", "")
            .replace(" ", "_")
            .replace("-", "_")
            .replace(".", "_")
            for x in name
        )
    else:  # Handle string column names
        new_name = (
            str(name)
            .replace("@", "")
            .replace("#", "")
            .replace(" ", "_")
            .replace("-", "_")
            .replace(".", "_")
        )  # More replacements as needed
    new_name = "".join(
        c for c in new_name if c.isalnum() or c == "_"
    )  # Remove non-alphanumeric
    if new_name[0].isdigit():
        new_name = "_" + new_name  # Add _ if starts with number
    return new_name


def get_relative_path(relative_path: str) -> Path:
    """Gets a path relative to the current file.

    Args:
        relative_path: The path relative to the current file.

    Returns:
        A pathlib.Path object representing the absolute path.
    """
    current_file_path = Path(__file__).resolve()
    current_dir = current_file_path.parent
    target_path = current_dir / relative_path
    return target_path.resolve()


def format_currency(amount, locale_str="en_US"):  # Default to US English
    """Formats a number as a currency string."""
    try:
        locale.setlocale(locale.LC_ALL, locale_str)  # Set the locale
        return locale.currency(
            amount, grouping=True
        )  # grouping=True adds commas/thousands separators
    except locale.Error:
        print(f"Locale '{locale_str}' not supported. Using default formatting.")
        return "${:,.2f}".format(amount)


def load_fpds_data_from_directory(data_directory: str) -> list[dict]:
    """
    Load FPDS data from JSON files in the specified directory.

    This function iterates through all JSON files in the given directory,
    reads each file, and aggregates the JSON data into a single list of dictionaries.

    Args:
        data_directory: The path to the directory containing JSON files.

    Returns:
        A list of FPDS entries (as nested dictionaries) loaded from all JSON files.
        Returns an empty list if no JSON files are found or if an error occurs during loading.
    """
    data = []
    data_path = Path(get_relative_path(data_directory))

    if not data_path.is_dir():
        print(f"Error: Directory '{data_directory}' not found.")
        return []

    json_files = sorted(
        list(data_path.glob("*.json"))
    )  # Sort files for consistent loading if needed
    if not json_files:
        print(f"Warning: No JSON files found in directory '{data_directory}'.")
        return []

    for file_path in json_files:
        try:
            with open(file_path, "r") as f:
                # Load JSON data from each file incrementally.
                # For very large files, consider using a streaming JSON parser if this becomes a bottleneck.
                file_data = json.load(f)
                if isinstance(
                    file_data, list
                ):  # Assuming each file contains a list of records
                    data.extend(file_data)
                elif isinstance(
                    file_data, dict
                ):  # Or if each file is a dict, handle accordingly
                    data.append(file_data)
                else:
                    print(
                        f"Warning: Unexpected JSON structure in file '{file_path}'. Expected list or dict at top level."
                    )
        except json.JSONDecodeError:
            print(
                f"Error: Could not decode JSON from file '{file_path}'. Skipping this file."
            )
        except Exception as e:
            print(
                f"Error: An unexpected error occurred while reading '{file_path}': {e}. Skipping this file."
            )

    return data


def get_canceled_contracts(df: pd.DataFrame):
    """Filter the dataframe to get contracts that have been terminated for convenience."""
    reason_tuple = (
        "entry",
        "content",
        "award",
        "contractData",
        "reasonForModification",
        "attributes",
        "description",
    )
    reason_value = "TERMINATE FOR CONVENIENCE (COMPLETE OR PARTIAL)"

    mask = (df[reason_tuple].fillna("")).iloc[:, 0] == reason_value
    filtered_df: pd.DataFrame = df.loc[mask]

    filtered_df = filtered_df.reset_index(drop=True)  # Reset index

    return filtered_df


def get_total_obligated_amount(df: pd.DataFrame):
    idx = pd.IndexSlice
    obligated_amount = df.loc[
        :, idx[:, :, "award", :, "totalObligatedAmount"]
    ].squeeze()
    obligated_amount = pd.to_numeric(obligated_amount, errors="coerce")
    return obligated_amount.sum()


def get_total_contract_value(df: pd.DataFrame):
    idx = pd.IndexSlice
    total_contract_value = df.loc[
        :, idx[:, :, "award", :, "totalBaseAndExercisedOptionsValue"]
    ].squeeze()
    total_contract_value = pd.to_numeric(total_contract_value, errors="coerce")
    return total_contract_value.sum()


def main():
    data_directory = "../../data"
    parquet_file = "sanitized_contract_data.parquet"
    parquet_path = Path(parquet_file)

    if parquet_path.exists():
        print(f"Loading data from existing parquet file: {parquet_file}")
        df = pd.read_parquet(parquet_file)
        new_columns = [tuple(col.split("_")) for col in df.columns]
        df.columns = pd.MultiIndex.from_tuples(new_columns)

    else:
        print("Parquet file not found. Loading and processing data from JSON files...")
        # Load the FPDS data from the directory
        data = load_fpds_data_from_directory(data_directory)

        df = pd.json_normalize(data, sep="_")
        new_columns = [
            tuple(sanitize_column_name(col) for col in col_tuple)
            if isinstance(col_tuple, tuple)
            else sanitize_column_name(col_tuple)
            for col_tuple in df.columns
        ]
        df.columns = pd.MultiIndex.from_tuples(new_columns)

        df.to_parquet(
            parquet_file,
            engine="pyarrow",
            compression="zstd",
            index=None,
        )
        print(f"Data processed and saved to parquet file: {parquet_file}")

    contracts = get_canceled_contracts(df)
    # print(contracts.info)

    summed_obligated_amount = get_total_obligated_amount(contracts)
    total_contract_value = get_total_contract_value(contracts)
    value_saved = format_currency(total_contract_value - summed_obligated_amount)

    data = [
        ["Total Contract Value", format_currency(total_contract_value)],
        ["Total Obligated Amount", format_currency(summed_obligated_amount)],
        ["Potential Savings", value_saved],
    ]

    table = tabulate(data, headers=["Metric", "Amount"], tablefmt="fancy_grid")

    title = "\033[1mDOGE Savings\033[0m"
    centered_title = title.center(
        len(table.splitlines()[0])
    )  # Center based on table width
    print("\n\n\n\n\n\n\n\n\n")
    print(centered_title)
    print("-" * len(table.splitlines()[0]))
    print(table)


if __name__ == "__main__":
    main()
