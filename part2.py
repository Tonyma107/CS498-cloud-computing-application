from flask import Flask
from google.cloud import bigtable
from google.cloud.bigtable import row_filters

app = Flask(__name__)

PROJECT_ID = "homework3-455720"
INSTANCE_ID = "ev-bigtable"
TABLE_ID = "ev-population"
COLUMN_FAMILY = "ev_info"

def get_bigtable_table():
    client = bigtable.Client(project=PROJECT_ID, admin=True)
    instance = client.instance(INSTANCE_ID)
    return instance.table(TABLE_ID)

@app.route('/rows')
def count_rows():
    table = get_bigtable_table()
    count = 0
    for row in table.read_rows():
        count += 1
    return str(count)


@app.route('/Best-BMW')
def best_bmw():
    table = get_bigtable_table()
    # Filter for BMW make and electric_range > 100
    bmw_filter = row_filters.ConditionalRowFilter(
        predicate_filter=row_filters.RowFilterChain(filters=[
            row_filters.ColumnQualifierRegexFilter(b"make"),
            row_filters.ValueRegexFilter(b"BMW")
        ]),
        true_filter=row_filters.RowFilterChain(filters=[
            row_filters.ColumnQualifierRegexFilter(b"electric_range"),
            row_filters.ValueRangeFilter(start_value=b"101")
        ])
    )
    count = 0
    for row in table.read_rows(filter_=bmw_filter):
        count += 1
    return str(count)


@app.route('/tesla-owners')
def tesla_seattle():
    table = get_bigtable_table()
    # Filter for Tesla make and Seattle city
    seattle_filter = row_filters.ConditionalRowFilter(
        predicate_filter=row_filters.RowFilterChain(filters=[
            row_filters.ColumnQualifierRegexFilter(b"make"),
            row_filters.ValueRegexFilter(b"Tesla")
        ]),
        true_filter=row_filters.RowFilterChain(filters=[
            row_filters.ColumnQualifierRegexFilter(b"city"),
            row_filters.ValueRegexFilter(b"Seattle")
        ])
    )
    count = 0
    for row in table.read_rows(filter_=seattle_filter):
        count += 1
    return str(count)


@app.route('/update')
def update_range():
    table = get_bigtable_table()
    target_id = "257246118"
    row_key = str(target_id).encode()
    row = table.read_row(row_key)
    if not row:
        return "Record not found", 404
    direct_row = table.direct_row(row_key)
    direct_row.set_cell(COLUMN_FAMILY, b"electric_range", str(200).encode("utf-8"))
    direct_row.commit()
    return "Success"

@app.route('/delete')
def delete_old():
    table = get_bigtable_table()
    batcher = table.mutations_batcher()
    total = 0
    deleted_count = 0
    for row in table.read_rows():
        total += 1
        year_cell = row.cells[COLUMN_FAMILY].get(b"model_year")
        if year_cell and int(year_cell[0].value) < 2014:
            direct_row = table.direct_row(row.row_key)
            direct_row.delete()
            batcher.mutate(direct_row)
            deleted_count += 1
    batcher.flush()
    remaining = total - deleted_count
    return str(remaining)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
