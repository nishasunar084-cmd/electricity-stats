#!/usr/bin/env python3
# start.py - Compute min, max, avg, total from electricity table.
# Usage examples:
#   python start.py --property 858 --group day
#   python start.py --group month --start 2025-01-01 --end 2025-03-31   (uses default property 858)

import mariadb
import sys
import argparse

# === CONFIGURATION – change if needed ===
DB_USER = "e2503360"
DB_PASSWORD = "6cgNNvykHBz"
DB_HOST = "mariadb.vamk.fi"
DB_PORT = 3306
DB_NAME = "e2503360_database"          
# ========================================

def get_connection():
    try:
        conn = mariadb.connect(
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME
        )
        return conn
    except mariadb.Error as e:
        print(f"Cannot connect to database '{DB_NAME}' on {DB_HOST}: {e}")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="Electricity statistics (min, max, avg, total)")
    parser.add_argument("--property", type=int, default=858,
                        help="Property ID (default: 858)")
    parser.add_argument("--group", choices=["day", "month", "year"], default="day",
                        help="Group by day, month, or year (default: day)")
    parser.add_argument("--start", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", help="End date (YYYY-MM-DD)")
    args = parser.parse_args()

    conn = get_connection()
    cursor = conn.cursor()

    # Choose grouping expression
    if args.group == "day":
        group_expr = "DATE(timestamp)"
    elif args.group == "month":
        group_expr = "DATE_FORMAT(timestamp, '%Y-%m')"
    else:  # year
        group_expr = "YEAR(timestamp)"

    # Build query (no joins, only electricity table)
    query = f"""
        SELECT
            {group_expr} AS period,
            MIN(value) AS min_value,
            MAX(value) AS max_value,
            AVG(value) AS avg_value,
            SUM(value) AS total_value
        FROM electricity
        WHERE property = ?
    """
    params = [args.property]

    if args.start:
        query += " AND timestamp >= ?"
        params.append(args.start)
    if args.end:
        query += " AND timestamp < DATE_ADD(?, INTERVAL 1 DAY)"
        params.append(args.end)

    query += " GROUP BY period ORDER BY period;"

    try:
        cursor.execute(query, params)
        rows = cursor.fetchall()
    except mariadb.Error as e:
        print(f"Query failed: {e}")
        sys.exit(1)

    if not rows:
        print("No data found for the given criteria.")
    else:
        print(f"\n Statistics for property {args.property} (grouped by {args.group}):")
        print("-" * 70)
        print(f"{'Period':<15} {'Min':>10} {'Max':>10} {'Avg':>10} {'Total':>12}")
        print("-" * 70)
        for row in rows:
            period = row[0]
            min_val = row[1]
            max_val = row[2]
            avg_val = row[3]
            total_val = row[4]
            print(f"{period:<15} {min_val:>10.2f} {max_val:>10.2f} {avg_val:>10.2f} {total_val:>12.2f}")
        print("-" * 70)

    cursor.close()
    conn.close()
conn = get_connection()
print(" Connected to database!")
conn.close()
if __name__ == "__main__":
    main()