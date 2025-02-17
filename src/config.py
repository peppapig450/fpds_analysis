from dataclasses import dataclass
from pathlib import Path
from typing import Self


@dataclass(frozen=True)
class Config:
    """Application configuration settings"""

    data_directory: Path
    parquet_filename: str
    use_live_data: bool = False
    locale_setting: str = "en_US"
    date_format: str = "%Y/%m/%d"

    @classmethod
    def from_env(cls) -> Self:
        """Create configuration from environment variables."""
        return cls(
            data_directory=Path("../data"),
            parquet_filename="../data/sanitized_contract_data.parquet",
            use_live_data=False,
        )
