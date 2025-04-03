import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, count, when, expr


def main(session: snowpark.Session):
    input_tables = [
        "<db>.<schema>.<table>",
        # Add more tables if needed
    ]

    result_rows = []

    # Pre-defined REGEX pattern for date-like values
    date_regex = (
        r"("
        r"^\d{4}[-/]\d{2}[-/]\d{2}$|"  # 2023-12-31 or 2023/12/31
        r"^\d{8}$|"  # 20231231
        r"^\d{2}[-/]\d{2}[-/]\d{4}$"  # 31-12-2023 or 31/12/2023
        r")"
    )

    for full_table_name in input_tables:
        database, schema, table = full_table_name.split(".")

        columns_info = session.sql(
            f"""
            SELECT column_name, data_type
            FROM {database}.information_schema.columns
            WHERE table_schema = '{schema}'
              AND table_name = '{table}'
        """
        ).collect()

        candidate_columns = [
            {"name": row["COLUMN_NAME"], "datatype": row["DATA_TYPE"]}
            for row in columns_info
            if row["DATA_TYPE"] in ("TEXT", "VARCHAR", "STRING", "CHAR")
        ]

        if candidate_columns:
            agg_exprs = [
                count(
                    when(
                        expr(
                            f"regexp_like({col['name']}, '{date_regex}') AND TRY_TO_DATE({col['name']}) IS NOT NULL"
                        ),
                        col["name"],
                    )
                ).alias(col["name"])
                for col in candidate_columns
            ]

            df = session.table(full_table_name).agg(*agg_exprs)
            df_result = df.collect()[0].as_dict()

            for col in candidate_columns:
                col_name = col["name"]
                if df_result.get(col_name, 0) > 0:
                    result_rows.append(
                        (database, schema, table, col_name, col["datatype"], "DATE")
                    )

    # ✅ Safe create_dataframe even if result_rows is empty
    if result_rows:
        result_df = session.create_dataframe(
            result_rows,
            schema=[
                "database",
                "schema",
                "table",
                "field",
                "datatype",
                "suggested_datatype",
            ],
        )
    else:
        # ✅ Return a dummy row with NULLs if empty
        result_df = session.create_dataframe(
            [(None, None, None, None, None, None)],
            schema=[
                "database",
                "schema",
                "table",
                "field",
                "datatype",
                "suggested_datatype",
            ],
        )

    result_df.show()
    return result_df
