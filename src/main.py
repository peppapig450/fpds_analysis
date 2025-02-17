import asyncio
from pathlib import Path

import pandas as pd

from config import Config
from data_loader import DataLoader
from data_processor import DataProcessor
from reporting import ContractReporter


async def main():
    config = Config.from_env()

    loader = DataLoader(config)
    processor = DataProcessor()
    reporter = ContractReporter(config)

    parquet_path = Path(config.parquet_filename)
    existing_df = None

    if parquet_path.exists() and not config.use_live_data:
        existing_df = pd.read_parquet(config.parquet_filename)
        if isinstance(existing_df.columns, pd.MultiIndex):
            df = existing_df
        else:
            new_columns = [tuple(col.split("_")) for col in existing_df.columns]
            existing_df.columns = pd.MultiIndex.from_tuples(new_columns)
            df = existing_df
    else:
        df = pd.DataFrame()

    data = (
        await loader.fetch_live_data()
        if config.use_live_data
        else loader.load_json_files()
    )

    if data:
        new_data_df = processor.create_dataframe(data)

        if existing_df is not None:
            print("Appending new data to existing DataFrame...")
            df = pd.concat([existing_df, new_data_df], ignore_index=True)
        else:
            df = new_data_df

        print(f"Saving updated parquet: {config.parquet_filename}")

        df.to_parquet(
            DataLoader.get_relative_path(config.parquet_filename),
            engine="pyarrow",
            compression="zstd",
            index=None,
        )
    else:
        if existing_df is not None:
            print("No new data loaded, using existing parquet data.")
            df = existing_df
        else:
            print("No new data loaded and no existing parquet file.")
            df = pd.DataFrame()

    if not df.empty:
        canceled_contracts = processor.get_canceled_contracts(df)
        obligated, total_value = processor.calcualte_contract_metrics(
            canceled_contracts
        )

        report = reporter.generate_savings_report(total_value, obligated)
        print(report)
    else:
        print("No data to analyze or report.")


if __name__ == "__main__":
    asyncio.run(main())
