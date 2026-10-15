"""Streams the Kaggle e-commerce transactions CSV into Kafka.

Every row becomes an ORDER event. Rows marked as returned also produce a RETURN_REQUEST event.
The category, date and return columns are detected from the CSV header; override them with
--category-col, --date-col and --return-col if detection picks the wrong one.

    python producer.py /data                    # the single .csv file in /data
    python producer.py /data/file.csv --rate 200   # throttle to 200 events/second for a live demo
"""
import argparse
import csv
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path

from kafka import KafkaProducer

from common import KAFKA_BOOTSTRAP, TOPIC, ensure_topic, retry

# Accepted header names, compared lower-case with spaces/underscores removed ("Product Category" -> "productcategory").
CATEGORY_NAMES = ["productcategory", "category", "categoryname", "productcategoryname", "producttype"]
DATE_NAMES = ["purchasedate", "orderdate", "transactiondate", "invoicedate", "orderdatetime", "date", "timestamp"]
RETURN_NAMES = ["returns", "returned", "returnstatus", "isreturned", "returnflag", "return", "returnrequested"]
RETURN_YES = {"1", "1.0", "true", "yes", "y", "returned", "return"}
DATE_FORMATS = ["%m/%d/%Y %H:%M", "%m/%d/%Y", "%d/%m/%Y %H:%M", "%d/%m/%Y", "%d-%m-%Y %H:%M", "%d-%m-%Y", "%Y/%m/%d"]


def normalize(name):
    return re.sub(r"[^a-z0-9]", "", name.lower())


def find_column(header, accepted, override, label):
    if override:
        if override not in header:
            sys.exit(f"Column '{override}' is not in the CSV. Columns: {header}")
        return override
    by_name = {normalize(h): h for h in header}
    for name in accepted:
        if name in by_name:
            return by_name[name]
    sys.exit(f"Could not find the {label} column. Columns: {header}\nPass it with --{label}-col \"<name>\".")


def parse_date(value, fmt):
    value = value.strip()
    if fmt is None:                       # ISO 8601: 2023-09-12, 2023-09-12 10:30:00, 2023-09-12T10:30:00Z
        return datetime.fromisoformat(value).date()
    return datetime.strptime(value, fmt).date()


def pick_date_format(samples):
    """Pick one format for the whole file so a date like 03/04/2023 is never read two different ways."""
    for fmt in [None] + DATE_FORMATS:
        try:
            for value in samples:
                parse_date(value, fmt)
            return fmt
        except ValueError:
            continue
    sys.exit(f"Unrecognised date format, e.g. {samples[:3]}. Pass --date-format, e.g. --date-format \"%d.%m.%Y\".")


def resolve_csv(path):
    path = Path(path)
    if path.is_file():
        return path
    files = sorted(path.glob("*.csv"))
    if len(files) != 1:
        sys.exit(f"Expected exactly one .csv file in {path}, found {len(files)}. Pass the file path instead.")
    return files[0]


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("csv_path", help="CSV file, or a folder containing exactly one CSV file")
    parser.add_argument("--category-col")
    parser.add_argument("--date-col")
    parser.add_argument("--return-col")
    parser.add_argument("--date-format", help="strptime format, if the date column is not detected automatically")
    parser.add_argument("--rate", type=float, default=0, help="events per second (default: as fast as possible)")
    parser.add_argument("--limit", type=int, default=0, help="stop after this many rows (default: all)")
    args = parser.parse_args()

    csv_file = resolve_csv(args.csv_path)
    with open(csv_file, newline="", encoding="utf-8-sig", errors="replace") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames or []
        category_col = find_column(header, CATEGORY_NAMES, args.category_col, "category")
        date_col = find_column(header, DATE_NAMES, args.date_col, "date")
        return_col = find_column(header, RETURN_NAMES, args.return_col, "return")
        samples = [row[date_col] for _, row in zip(range(1000), reader) if (row[date_col] or "").strip()]
    date_format = args.date_format or pick_date_format(samples)
    print(f"Reading {csv_file.name}: category='{category_col}', date='{date_col}', return='{return_col}', "
          f"date format={date_format or 'ISO'}", flush=True)

    ensure_topic()
    producer = retry(lambda: KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        key_serializer=str.encode,                                  # key = category -> same partition per category
        value_serializer=lambda event: json.dumps(event).encode(),
        linger_ms=20,
        batch_size=128 * 1024,
    ), "Kafka")

    rows = orders = returns = skipped = 0
    started = time.time()
    with open(csv_file, newline="", encoding="utf-8-sig", errors="replace") as f:
        for row in csv.DictReader(f):
            if args.limit and rows >= args.limit:
                break
            rows += 1
            category = " ".join((row[category_col] or "").split()).replace("|", "/")   # no tabs/newlines/pipes
            try:
                event_date = parse_date(row[date_col] or "", date_format).isoformat()
            except ValueError:
                event_date = None
            if not category or not event_date:
                skipped += 1
                continue

            event = {"event_type": "ORDER", "category": category, "event_date": event_date, "row": rows}
            producer.send(TOPIC, key=category, value=event)
            orders += 1
            if (row[return_col] or "").strip().lower() in RETURN_YES:
                producer.send(TOPIC, key=category, value={**event, "event_type": "RETURN_REQUEST"})
                returns += 1

            if args.rate:                                           # throttle for a "live stream" demo
                wait = started + (orders + returns) / args.rate - time.time()
                if wait > 0:
                    time.sleep(wait)
            if rows % 50000 == 0:
                print(f"  {rows:,} rows sent", flush=True)

    producer.flush()
    producer.close()
    print(f"Done in {time.time() - started:.1f}s: {orders:,} ORDER and {returns:,} RETURN_REQUEST events "
          f"sent to topic '{TOPIC}' ({skipped:,} rows skipped: missing category or date)", flush=True)


if __name__ == "__main__":
    main()
