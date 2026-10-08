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


def simulate_transactions():

    connection = create_connection()
    cursor = connection.cursor()

    processed_count = 0
    inserted_count = 0

    try:

        with open(CSV_PATH, "r", encoding="utf-8", newline="") as file:

            reader = csv.DictReader(file)

            for source_row_number, row in enumerate(reader, start=1):

                if (
                    MAX_TRANSACTIONS > 0
                    and processed_count >= MAX_TRANSACTIONS
                ):
                    break

                try:

                    inserted = insert_transaction(
                        cursor,
                        source_row_number,
                        row
                    )

                    connection.commit()

                    processed_count += 1

                    if inserted == 1:
                        inserted_count += 1

                        print(
                            f"[INSERTED] "
                            f"Row={source_row_number} | "
                            f"Type={row['type']} | "
                            f"Amount={row['amount']} | "
                            f"Fraud={row['isFraud']}"
                        )

                    else:

                        print(
                            f"[SKIPPED] "
                            f"Row={source_row_number} already exists."
                        )

                    time.sleep(
                        TRANSACTION_DELAY_SECONDS
                    )

                except Exception as error:

                    connection.rollback()

                    print(
                        f"[ERROR] "
                        f"Row={source_row_number} | "
                        f"{error}"
                    )

                    raise

    except KeyboardInterrupt:

        print("\nSimulator stopped by user.")

    finally:

        cursor.close()
        connection.close()

        print("\nSimulation finished.")
        print(f"Processed: {processed_count}")
        print(f"Inserted: {inserted_count}")


if __name__ == "__main__":
    simulate_transactions()
