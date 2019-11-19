from io import StringIO
import pandas as pd
import numpy as np


def _create_forward_filled_column(str_line):
    return pd.Series(str_line.split(';')).replace('', np.nan).ffill()


def _create_forward_filled_columns(str_lines, skip_header_rows=0):
    if skip_header_rows > 0:
        if skip_header_rows >= len(str_lines):
            raise ValueError("Too many header rows to skip: %s. Maximum is: %s." %
                             (str(skip_header_rows), str(len(str_lines) - 1)))
        str_lines = str_lines[skip_header_rows:]

    if len(str_lines) == 0:
        raise ValueError("No header rows found.")

    columns = _create_forward_filled_column(str_lines[0])
    for str_line in str_lines[1:]:
        columns = [(str(elem1) + '.' + str(elem2)) if not pd.isnull(elem1) else np.nan
                   for elem1, elem2 in zip(columns, _create_forward_filled_column(str_line))]
    return columns


def parse_csv(csv_string, na_values, skip_header_rows=0):
    lines = csv_string.splitlines()

    # Detect header lines
    header_start_index = None
    data_start_index = None
    line_nr = 0
    for line in lines:

        # Find first line with header info
        if header_start_index is None and line.startswith(';'):
            header_start_index = line_nr

        # Find first line with data
        if header_start_index is not None and not line.startswith(';'):
            data_start_index = line_nr
            break
        line_nr += 1

    # Parse csv data
    df = pd.read_csv(StringIO(csv_string), sep=';', decimal=',', skiprows=data_start_index - 1, na_values=na_values)

    # Cut footer lines
    first_footer_line_index = None
    for i in reversed(df.index):
        if df.loc[i, df.columns[0]].startswith('___'):
            first_footer_line_index = i
            break
    if first_footer_line_index is not None:
        df = df.loc[:first_footer_line_index - 1]

    # Parse and concat columns
    columns = _create_forward_filled_columns(lines[header_start_index:data_start_index],
                                             skip_header_rows=skip_header_rows)

    # Mark empty columns as index
    empty_column_nr = 0
    for i in range(len(columns)):
        if pd.isnull(columns[i]):
            columns[i] = 'index.' + str(empty_column_nr)
            empty_column_nr += 1
    df.columns = columns

    return df
