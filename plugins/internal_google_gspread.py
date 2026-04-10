import logging
import pandas as pd
import gspread
import requests

from google.auth import default
from google.auth.exceptions import RefreshError
from gspread.exceptions import APIError, WorksheetNotFound

class InternalGspreadExtractor:
    """
    Internal Generic Google Sheets Extractor
    ---
    Direction format:
        <spreadsheet_id>.<worksheet_name>
    ---
    Features:
        - Retryable error handling
        - Clean DataFrame normalization
        - Airflow-friendly exception design
    """

    RETRYABLE_STATUS = {408, 429, 500, 502, 503, 504}
    NON_RETRYABLE_STATUS = {401, 403}

    def __init__(self, direction: str):

        self.direction = direction

        scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

        try:
            creds, _ = default(scopes=scopes)

            self.client = gspread.authorize(creds)

            logging.info(
                f"✅ [INIT] Initialized Gspread client with scopes {scopes}"
            )

        except Exception as e:

            error = RuntimeError(
                f"❌ [INIT] Failed to initialize Gspread client due to {e}"
            )
            error.retryable = False
            raise error from e

    # ========================
    # 1. Parse direction
    # ========================
    def _parse_direction(self):

        try:
            spreadsheet_id, worksheet_name = self.direction.split(".", 1)
            return spreadsheet_id, worksheet_name

        except ValueError as e:

            error = RuntimeError(
                f"❌ [PARSE] Invalid direction format {self.direction}. "
                "Expected <spreadsheet_id>.<worksheet_name>"
            )
            error.retryable = False
            raise error from e

    # ========================
    # 2. Fetch raw values
    # ========================
    def _fetch_raw_values(self, spreadsheet_id, worksheet_name):

        try:
            logging.info(
                f"🔍 [FETCH] Reading {spreadsheet_id}.{worksheet_name}"
            )

            sheet = self.client.open_by_key(spreadsheet_id)
            worksheet = sheet.worksheet(worksheet_name)

            values = worksheet.get_all_values()

            return values

        except WorksheetNotFound as e:

            error = RuntimeError(
                f"❌ [FETCH] Worksheet {worksheet_name} not found in {spreadsheet_id}"
            )
            error.retryable = False
            raise error from e

        except RefreshError as e:

            error = RuntimeError(
                "❌ [FETCH] Unauthorized Google credentials - re-auth required"
            )
            error.retryable = False
            raise error from e

        except APIError as e:

            status = e.response.status_code if e.response else None

            if status in self.RETRYABLE_STATUS:

                error = RuntimeError(
                    f"⚠️ [FETCH] Retryable API error {status}: {e}"
                )
                error.retryable = True
                raise error from e

            if status in self.NON_RETRYABLE_STATUS:

                error = RuntimeError(
                    f"❌ [FETCH] Unauthorized API error {status}: {e}"
                )
                error.retryable = False
                raise error from e

            error = RuntimeError(
                f"❌ [FETCH] Non-retryable API error {status}: {e}"
            )
            error.retryable = False
            raise error from e

        except requests.exceptions.Timeout as e:

            error = RuntimeError(
                f"⚠️ [FETCH] Timeout error: {e}"
            )
            error.retryable = True
            raise error from e

        except requests.exceptions.ConnectionError as e:

            error = RuntimeError(
                f"⚠️ [FETCH] Connection error: {e}"
            )
            error.retryable = True
            raise error from e

        except Exception as e:

            error = RuntimeError(
                f"❌ [FETCH] Unknown error: {e}"
            )
            error.retryable = False
            raise error from e

    # ========================
    # 3. Normalize DataFrame
    # ========================
    def _normalize_dataframe(self, values):

        if not values:
            logging.warning("⚠️ [NORMALIZE] Empty dataset")
            return pd.DataFrame()

        headers = [str(col).strip() for col in values[0]]
        rows_raw = values[1:]

        rows_clean = [
            row for row in rows_raw
            if any(str(cell).strip() != "" for cell in row)
        ]

        rows = []
        for row in rows_clean:
            padded = row + [""] * (len(headers) - len(row))
            rows.append(padded[:len(headers)])

        df = pd.DataFrame(rows, columns=headers).astype("string")

        for col in df.columns:
            df[col] = df[col].str.strip()

        return df

    # ========================
    # 4. Public API
    # ========================
    def fetch(self) -> pd.DataFrame:

        spreadsheet_id, worksheet_name = self._parse_direction()

        values = self._fetch_raw_values(
            spreadsheet_id,
            worksheet_name
        )

        df = self._normalize_dataframe(values)

        logging.info(
            f"✅ [FETCH] Loaded {len(df):,} rows from "
            f"{spreadsheet_id}.{worksheet_name}"
        )

        return df