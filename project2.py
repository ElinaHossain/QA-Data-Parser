import argparse
import pymongo
import openpyxl
import csv
from datetime import datetime

"""
commands for terminal:

python project2.py --files EG4-DBDump_Spring2024.xlsx --db db1
python project2.py --files EG4-DBDump_Fall2024.xlsx --db db2

python project2.py --user "Kevin Chaja" --csv
# Console output: Exported 25 records to output.csv

python project2.py --repeatable --csv  
# Console output: Exported 1231 records to output.csv

python project2.py --blocker --csv
# Console output: Exported 493 records to output.csv

python project2.py --repeatable --blocker --csv
# Console output: Exported 394 records to output.csv

python project2.py --date "2/24/2024" --csv
# Console output: Exported 52 records to output.csv
"""

cli = argparse.ArgumentParser(description="Process Excel files into MongoDB and perform queries.")
cli.add_argument("--files", nargs='+', help="Excel files to process")
cli.add_argument("--db", choices=['db1', 'db2'], help="MongoDB collection")
cli.add_argument("--user", help="Test Owner filter")
cli.add_argument("--repeatable", action="store_true", help="Filter repeatable bugs")
cli.add_argument("--blocker", action="store_true", help="Filter blocker bugs")
cli.add_argument("--date", help="Filter by date MM/DD/YYYY")
cli.add_argument("--csv", action="store_true", help="Export results to CSV")
args = cli.parse_args()

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
database = myclient["project2"]

def fix_date(input_date):
    try:
        if isinstance(input_date, datetime):
            return input_date.strftime("%m/%d/%Y")
        input_date = input_date.split()[0].replace("-", "/").replace(".", "/")
        return datetime.strptime(input_date, "%m/%d/%Y").strftime("%m/%d/%Y")
    except:
        return input_date

if args.files and args.db:
    coll = database[args.db]
    coll.delete_many({})
    for file_name in args.files:
        workbook = openpyxl.load_workbook(file_name)
        sheet = workbook.active
        keys = [cell.value for cell in sheet[1]]

        for row in sheet.iter_rows(min_row=2, values_only=True):
            row_data = {keys[i]: fix_date(row[i]) if keys[i] == "Build #" else row[i] for i in range(len(keys))}
            coll.insert_one(row_data)

coll_list = [database["db1"], database["db2"]]
results = []

search_params = {}
if args.user:
    search_params["Test Owner"] = args.user.strip()
if args.date:
    search_params["Build #"] = fix_date(args.date)

for collection in coll_list:
    results.extend(collection.find(search_params))

final_results = []
unique_set = set()

for item in results:
    rep_val = str(item.get("Repeatable?", "")).lower().startswith("y")
    blk_val = str(item.get("Blocker?", "")).lower().startswith("y")

    if args.repeatable and not rep_val:
        continue
    if args.blocker and not blk_val:
        continue

    unique_key = (item.get("Test Case"), item.get("Test Owner"), item.get("Build #"))
    if unique_key not in unique_set:
        unique_set.add(unique_key)
        item.pop("_id", None)
        final_results.append(item)

if args.csv:
    csv_file = "output.csv"  
    
    if final_results:
        keys = final_results[0].keys()
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(final_results)
        print(f"Exported {len(final_results)} records to {csv_file}")
    else:
        print("No results to export.")

myclient.close()