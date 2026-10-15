import csv
import os
import time
from decimal import Decimal
from pathlib import Path

from dotenv import load_dotenv
import pyodbc

load_dotenv()


CSV_FILE = os.getenv(
    "CSV_FILE",
    "data/PS_20174392719_1491204439457_log.csv"
)
CSV_PATH = Path(CSV_FILE)
if not CSV_PATH.is_absolute():
    CSV_PATH = Path(__file__).resolve().parent.parent / CSV_PATH


SQL_SERVER = os.getenv("SQL_SERVER", "localhost,1433")
DATABASE = os.getenv("DATABASE", "FraudDB")
USERNAME = os.getenv("DB_USER", "sa")
PASSWORD = os.getenv("DB_PASSWORD")

if not PASSWORD:
  raise ValueError(
      "DB_PASSWORD environment variable is required from .env file."
  )


TRANSACTION_DELAY_SECONDS = float(
    os.getenv("TRANSACTION_DELAY_SECONDS", "1")
)

MAX_TRANSACTIONS = int(
    os.getenv("MAX_TRANSACTIONS", "100")
)
CONNECT_RETRY_SECONDS = float(os.getenv("CONNECT_RETRY_SECONDS", "5"))
CONNECT_MAX_ATTEMPTS = int(os.getenv("CONNECT_MAX_ATTEMPTS", "60"))


CONNECTION_STRING = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER={SQL_SERVER};"
    f"DATABASE={DATABASE};"
    f"UID={USERNAME};"
    f"PWD={PASSWORD};"
    "TrustServerCertificate=yes;"
)


INSERT_QUERY = """
INSERT INTO dbo.Transactions
(
    SourceRowNumber,
    Step,
    TransactionType,
    Amount,
    NameOrig,
    OldBalanceOrg,
    NewBalanceOrig,
    NameDest,
    OldBalanceDest,
    NewBalanceDest,
    IsFraud,
    IsFlaggedFraud
)
SELECT
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?
WHERE NOT EXISTS
(
    SELECT 1
    FROM dbo.Transactions
    WHERE SourceRowNumber = ?
);
"""

CHECKPOINT_QUERY = """
UPDATE dbo.SimulatorCheckpoint
SET SourceRowNumber = ?, NextByteOffset = ?, UpdatedAt = SYSUTCDATETIME()
WHERE SourcePath = ?;
IF @@ROWCOUNT = 0
BEGIN
    INSERT INTO dbo.SimulatorCheckpoint
        (SourcePath, SourceRowNumber, NextByteOffset)
    VALUES (?, ?, ?);
END;
"""


class Utf8LineIterator:
    """Expose decoded physical CSV lines while retaining binary seek offsets."""

    def __init__(self, binary_file):
        self.binary_file = binary_file

    def __iter__(self):
        return self

    def __next__(self):
        raw_line = self.binary_file.readline()
        if not raw_line:
            raise StopIteration
        return raw_line.decode("utf-8")


def create_connection():
    last_error = None
    for attempt in range(1, CONNECT_MAX_ATTEMPTS + 1):
        try:
            return pyodbc.connect(CONNECTION_STRING, timeout=5)
        except pyodbc.Error as error:
            last_error = error
            if attempt == CONNECT_MAX_ATTEMPTS:
                break
            wait_seconds = min(CONNECT_RETRY_SECONDS, 30)
            print(
                f"SQL Server is not ready ({attempt}/{CONNECT_MAX_ATTEMPTS}); "
                f"retrying in {wait_seconds:g}s: {error}"
            )
            time.sleep(wait_seconds)

    raise RuntimeError(
        f"Could not connect to {SQL_SERVER}/{DATABASE} after "
        f"{CONNECT_MAX_ATTEMPTS} attempts. Confirm Docker is healthy and "
        "sql-init completed successfully."
    ) from last_error


def insert_transaction(cursor, source_row_number, row):
    cursor.execute(
        INSERT_QUERY,

        source_row_number,

        int(row["step"]),
        row["type"],

        Decimal(row["amount"]),

        row["nameOrig"],
        Decimal(row["oldbalanceOrg"]),
        Decimal(row["newbalanceOrig"]),

        row["nameDest"],
        Decimal(row["oldbalanceDest"]),
        Decimal(row["newbalanceDest"]),

        int(row["isFraud"]),
        int(row["isFlaggedFraud"]),

        source_row_number
    )

    return cursor.rowcount

def get_last_processed_row(cursor):
    cursor.execute("SELECT ISNULL(MAX(SourceRowNumber), 0) FROM dbo.Transactions")
    result = cursor.fetchone()
    return result[0] if result else 0


def get_checkpoint(cursor):
    cursor.execute(
        "SELECT SourceRowNumber, NextByteOffset "
        "FROM dbo.SimulatorCheckpoint WHERE SourcePath = ?",
        str(CSV_PATH.resolve()),
    )
    return cursor.fetchone()


def save_checkpoint(cursor, connection, source_row_number, next_byte_offset):
    source_path = str(CSV_PATH.resolve())
    cursor.execute(
        CHECKPOINT_QUERY,
        source_row_number,
        next_byte_offset,
        source_path,
        source_path,
        source_row_number,
        next_byte_offset,
    )
    connection.commit()

def simulate_transactions():
    connection = create_connection()
    cursor = connection.cursor()
    processed_count = 0
    inserted_count = 0

    try:
        with open(CSV_PATH, "rb") as file:
            lines = Utf8LineIterator(file)
            reader = csv.DictReader(lines)
            headers = reader.fieldnames
            if not headers:
                raise ValueError(f"CSV file is empty or has no header: {CSV_PATH}")

            checkpoint = get_checkpoint(cursor)
            if checkpoint:
                last_row, next_byte_offset = checkpoint
                file.seek(next_byte_offset)
                reader = csv.DictReader(lines, fieldnames=headers)
            else:
                # One-time bootstrap for databases populated before byte-offset
                # checkpoints were introduced.
                last_row = get_last_processed_row(cursor)
                if last_row:
                    for _ in range(last_row):
                        if next(reader, None) is None:
                            raise ValueError(
                                "Database watermark exceeds the number of CSV records."
                            )
                    next_byte_offset = file.tell()
                    save_checkpoint(cursor, connection, last_row, next_byte_offset)
                else:
                    next_byte_offset = file.tell()

            print(
                f"Database watermark is {last_row}. "
                f"Resuming from CSV row {last_row + 1}."
            )

            for source_row_number, row in enumerate(reader, start=last_row + 1):
                if MAX_TRANSACTIONS > 0 and processed_count >= MAX_TRANSACTIONS:
                    break

                try:
                    inserted = insert_transaction(cursor, source_row_number, row)
                    next_byte_offset = file.tell()
                    source_path = str(CSV_PATH.resolve())
                    cursor.execute(
                        CHECKPOINT_QUERY,
                        source_row_number,
                        next_byte_offset,
                        source_path,
                        source_path,
                        source_row_number,
                        next_byte_offset,
                    )
                    connection.commit()

                    processed_count += 1
                    if inserted == 1:
                        inserted_count += 1
                        print(f"[INSERTED] Row={source_row_number} | Type={row['type']} | Amount={row['amount']} | Fraud={row['isFraud']}")
                    else:
                        print(f"[SKIPPED] Row={source_row_number} already exists.")

                    time.sleep(TRANSACTION_DELAY_SECONDS)

                except Exception as error:
                    connection.rollback()
                    print(f"[ERROR] Row={source_row_number} | {error}")
                    raise

    except KeyboardInterrupt:
        print("\nSimulator stopped by user.")
    finally:
        cursor.close()
        connection.close()
        print("\nSimulation finished.")
        print(f"Processed in this run: {processed_count}")
        print(f"Inserted in this run: {inserted_count}")


if __name__ == "__main__":
    simulate_transactions()
