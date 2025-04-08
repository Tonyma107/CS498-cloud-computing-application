import csv
from google.cloud import bigtable

def main():
    client = bigtable.Client(project="homework3-455720", admin=True)
    instance = client.instance("ev-bigtable")
    table = instance.table("ev-population")
    batcher = table.mutations_batcher()

    with open("Electric_Vehicle_Population_Data.csv", "r") as csvfile:
        reader = csv.DictReader(csvfile)
        for i, row in enumerate(reader):
            try:
                row_key = str(row["DOL Vehicle ID"]).encode()
                direct_row = table.direct_row(row_key)

                direct_row.set_cell("ev_info", b"make", row["Make"])
                direct_row.set_cell("ev_info", b"model", row["Model"])
                direct_row.set_cell("ev_info", b"model_year", row["Model Year"])
                direct_row.set_cell("ev_info", b"electric_range", str(int(row["Electric Range"].strip() or 0)))
                direct_row.set_cell("ev_info", b"city", row["City"])
                direct_row.set_cell("ev_info", b"county", row["County"])

                batcher.mutate(direct_row)

                if i % 5000 == 0:
                    batcher.flush()
                    print(f"Processed {i} rows")

            except Exception as e:
                print(f"Error at row {i}: {e}")

        batcher.flush()
        print("Upload completed.")

if __name__ == "__main__":
    main()

