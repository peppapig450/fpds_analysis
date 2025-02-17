from typing import Any

import pandas as pd
from pandas import DataFrame, MultiIndex


class DataProcessor:
    """Handles data processing and transformation operations."""

    @staticmethod
    def sanitize_column_names(name: str):
        """Sanitize column names for valid Python identifiers."""
        if isinstance(name, tuple):
            return tuple(DataProcessor.sanitize_column_names(x) for x in name)

        new_name = (
            str(name)
            .replace("@", "")
            .replace("#", "")
            .replace(" ", "_")
            .replace("-", "_")
            .replace(".", "_")
        )

        new_name = "".join(c for c in new_name if c.isalnum() or c == "_")
        return f"_{new_name}" if new_name[0].isdigit() else new_name

    @classmethod
    def create_dataframe(cls, data: list[dict[Any, Any]]) -> DataFrame:
        """Create and prepare a dataframe from raw data."""
        df = pd.json_normalize(data, sep="_")
        new_columns = [cls.sanitize_column_names(col) for col in df.columns]
        df.columns = MultiIndex.from_tuples(
            [tuple(col.split("_")) for col in new_columns]  #type: ignore
        )

        return df

    @staticmethod
    def get_canceled_contracts(df: DataFrame) -> DataFrame:
        """Filter for terminated contracts."""
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

        mask = df[reason_tuple].fillna("").iloc[:, 0] == reason_value  # type: ignore
        return df.loc[mask].reset_index(drop=True)

    @staticmethod
    def calcualte_contract_metrics(df: DataFrame) -> tuple[float, float]:
        """Calculate key contract metrics."""
        idx = pd.IndexSlice

        obligated: float = pd.to_numeric(
            df.loc[:, idx[:, :, "award", :, "totalObligatedAmount"]].squeeze(),  # type: ignore
            errors="coerce",
        ).sum()

        total_value: float = pd.to_numeric(
            df.loc[
                :, idx[:, :, "award", :, "totalBaseAndExercisedOptionsValue"]
            ].squeeze(),  # type: ignore
            errors="coerce",
        ).sum()

        return obligated, total_value
