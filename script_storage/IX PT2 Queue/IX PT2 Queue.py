import datetime as dt
from utilities.oracle_bridge import update_table, run_query, get_data_from_table

sql_file = 'IX Queuev6_Final.sql'
table_name = 'JDLAURET.T_IX_PT2_QUEUE_H'
encoding = 'utf-8'

if __name__ == '__main__':
    # Generate Load Date
    load_date = dt.datetime.now()
    load_date = load_date.replace(minute=0, second=0, microsecond=0)
    print('Load Date set to {}'.format(str(load_date)))

    print('Opening Sql File')
    # Open Sql file read all lines and join into string
    with open(sql_file, 'r') as infile:
        sql = ' '.join(infile.readlines())

    print('Getting Query Results')
    # Get Query Data
    results = run_query('', raw_query=sql, credentials='private', encoding=encoding)

    # Append Load Date to all rows
    for i, result in enumerate(results):
        results[i].append(load_date)

    print('Getting Existing Data')
    # Get all existing data for comparison
    existing_data = get_data_from_table(table_name, credentials='private')

    # Remove all duplicate lines from results
    print('Searching for and removing duplicates')
    results = [x for x in results if x not in existing_data]
    del results[0]

    # Upload remaining results if any to table
    if len(results) > 0:
        print('Uploading Data to Table')
        update_table(table_name, results, header_included=False,
                     date_time=True, credentials='private', encoding=encoding)