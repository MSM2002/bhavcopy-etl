# Scaffold for making the orchestrating class
"""
from datetime import timedelta
import polars as pl

from bhavcopy-etl.playwright_utils import fetch_headers
from bhavcopy-etl.downloader_async import (
    download_bhavcopies,
    download_corporate_actions,
    download_delisted_companies,
)
from bhavcopy-etl.adjustments import apply_adjustments
from bhavcopy-etl.parquet_utils import write_parquet_file
from bhavcopy-etl.calendar import get_last_trading_day


class BhavcopyETL:
    def __init__(self, previous_file: str, min_period: int = 365):
        self.previous_file = previous_file
        self.min_period = min_period
        self.headers = None
        self.previous_df: pl.LazyFrame | None = None

    def _load_previous_data(self):
        try:
            self.previous_df = pl.scan_parquet(self.previous_file)
        except FileNotFoundError:
            self.previous_df = None

    def _determine_date_range(self):
        last_trading_day = get_last_trading_day()

        if self.previous_df is None:
            start_date = last_trading_day - timedelta(days=self.min_period)
        else:
            # Collect just the max date from LazyFrame
            prev_max_date = self.previous_df.select(pl.col("DATE").max()).collect()[0, 0]
            start_date_candidate = prev_max_date + timedelta(days=1)
            min_start = last_trading_day - timedelta(days=self.min_period)
            start_date = min(start_date_candidate, min_start)

        return start_date, last_trading_day

    async def _fetch_headers(self):
        self.headers = await fetch_headers()

    async def _download_files(self, start_date, end_date):
        bhav_df = await download_bhavcopies(start_date, end_date, self.headers)
        corp_df = await download_corporate_actions(start_date, end_date, self.headers)
        delisted_df = await download_delisted_companies(self.headers)

        return {
            "bhavcopies": bhav_df,           # LazyFrame
            "corporate_actions": corp_df,    # LazyFrame
            "delisted": delisted_df          # LazyFrame
        }

    def _process_data(self, data: dict) -> pl.LazyFrame:
        bhav_df = data["bhavcopies"]
        corp_df = data["corporate_actions"]
        delisted_df = data["delisted"]

        adjusted_df = apply_adjustments(bhav_df, corp_df, delisted_df, self.previous_df)

        return adjusted_df

    def _write_data(self, df: pl.LazyFrame):
        # Materialize and write to disk
        write_parquet_file(df.collect(), self.previous_file)

    def run(self):
        import asyncio
        asyncio.run(self._run())

    async def _run(self):
        self._load_previous_data()
        start_date, end_date = self._determine_date_range()
        await self._fetch_headers()
        downloaded_data = await self._download_files(start_date, end_date)
        final_df = self._process_data(downloaded_data)
        self._write_data(final_df)

"""