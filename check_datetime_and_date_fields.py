import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, count, when


def main(session: snowpark.Session):
    input_tables = [
        "DEV_PRISM.RAW_CHIRAG_DATA_MODELS.FACT_ORDER_ITEM",
        # Add more tables here if needed
    ]

    result_rows = []

    for full_table_name in input_tables:
        database, schema, table = full_table_name.split(".")

        # Step 1: Get VARCHAR-like columns with "date" in the name
        columns_info = session.sql(
            f"""
            SELECT column_name, data_type
            FROM {database}.information_schema.columns
            WHERE table_schema = '{schema}'
              AND table_name = '{table}'
              AND data_type IN ('VARCHAR', 'TEXT', 'STRING', 'CHAR')
              AND column_name ILIKE '%date%'
        """
        ).collect()

        if not columns_info:
            continue

        date_like_columns = [row["COLUMN_NAME"] for row in columns_info]
        col_exprs = [
            count(when(col(c).is_not_null(), c)).alias(c) for c in date_like_columns
        ]

        # Step 2: Count non-null values for each date-named column
        df = session.table(full_table_name).agg(*col_exprs)
        df_result = df.collect()[0].as_dict()

        for col_name in date_like_columns:
            if df_result.get(col_name, 0) == 0:
                col_info = next(c for c in columns_info if c["COLUMN_NAME"] == col_name)
                result_rows.append(
                    (
                        database,
                        schema,
                        table,
                        col_name,
                        col_info["DATA_TYPE"],
                        "empty date field",
                    )
                )

    # Final result dataframe
    if result_rows:
        result_df = session.create_dataframe(
            result_rows,
            schema=["database", "schema", "table", "column", "datatype", "reason"],
        )
    else:
        result_df = session.create_dataframe(
            [(None, None, None, None, None, None)],
            schema=["database", "schema", "table", "column", "datatype", "reason"],
        )

    result_df.show()
    return result_df
