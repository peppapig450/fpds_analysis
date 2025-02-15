import asyncio
import datetime

import pandas as pd
from fpds import fpdsRequest


async def fetch_fpds_data_for_today() -> list[dict]:
    """
    Fetch FPDS data for today's date.

    We assume that the FPDS feed accepts a LAST_MOD_DATE parameter formatted as:
        LAST_MOD_DATE=[YYYY/MM/DD, YYYY/MM/DD]
    where both dates are set to today's date.

    Returns:
        A list of FPDS entries (as nested dictionaries).
    """
    today = datetime.date.today().strftime("%Y/%m/%d")
    date_range = f"[{"2025/02/12"}, {today}]"

    # Instantiate the request with today's modification date.
    # Set cli_run=True to bypass extra parameter validations.
    request = fpdsRequest(LAST_MOD_DATE=date_range, cli_run=True)

    # Retrieve the data asynchronously.
    data = await request.data()
    return data


def main():
    # Fetch the FPDS data for today asynchronously.
    data = asyncio.run(fetch_fpds_data_for_today())

    df = pd.json_normalize(data, sep="_")
    new_columns = [tuple(col.split("_")) for col in df.columns]
    df.columns = pd.MultiIndex.from_tuples(new_columns)

    print("Data loaded into pandas DataFrame:")
    print(df.head())
    print(df.info(verbose=True))
    
if __name__ == "__main__":
    main()
