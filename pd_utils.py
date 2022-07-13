import logging
import json
import pandas as pd
import settings


def generate_bq_schema(
    dataframe: pd.DataFrame, default_type="STRING", record_cols=None
) -> dict:
    """Given a passed dataframe, generate the associated Google BigQuery schema.
    Arguments:
        dataframe (pandas.DataFrame): D
    default_type : string
        The default big query type in case the type of the column
        does not exist in the schema.
    """

    # If you update this mapping, also update the table at
    # `docs/source/writing.rst`.
    type_mapping = {
        "i": "INTEGER",
        "b": "BOOLEAN",
        "f": "FLOAT",
        "O": "STRING",
        "S": "STRING",
        "U": "STRING",
        "M": "TIMESTAMP",
    }

    if not record_cols:
        record_cols = check_cols_for_list(dataframe)
    fields = []
    for column_name, dtype in dataframe.dtypes.items():
        if record_cols is not None and column_name in record_cols:
            fields.append({"name": column_name, "type": "RECORD", "mode": "REPEATED"})
            continue
        fields.append(
            {"name": column_name, "type": type_mapping.get(dtype.kind, default_type)}
        )

    return fields


def check_cols_for_list(df):
    columns = df.select_dtypes(["object"]).columns
    columns_w_list = []
    for column in columns:
        c = df[column]
        for v in c:
            try:
                col_json = json.loads(v.replace("'", '"'))
                if col_json:
                    columns_w_list.append(column)
                    break
            except Exception as e:
                logging.info(e)
    return columns_w_list


def convert_col_to_json_array(column):
    import ast

    return [ast.literal_eval(c) for c in column]


def df_conversion(df, fn):
    schema = generate_bq_schema(df)
    settings.write_json_to_storage(schema, "data/schemas", fn)
    cols_w_list = check_cols_for_list(df)
    if cols_w_list:
        for col in cols_w_list:
            df[col] = convert_col_to_json_array(df[col])


def produce_schema_from_df(file: str) -> bool:
    itercsv = pd.read_csv(file, iterator=True, chunksize=1000)
    schema_file_prefix = "_".join(file.replace(".csv", "").split("/")[1:])
    for chunk in itercsv:
        schema = generate_bq_schema(chunk)
        settings.write_json_to_storage(
            schema, "data/schemas", fn=f"{schema_file_prefix}.json"
        )
        return True
