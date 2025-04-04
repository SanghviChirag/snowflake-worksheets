import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, count, when, expr


def main(session: snowpark.Session):
    input_tables = [
        "<db>.<schema>.<tbl>",
    ]

    result_rows = []

    # DATE format pattern
    date_regex = (
        "("
        "^\\d{4}[-/]\\d{2}[-/]\\d{2}$|"
        "^\\d{2}[-/]\\d{2}[-/]\\d{4}$|"
        "^20\\d{2}(0[1-9]|1[0-2])(0[1-9]|[12][0-9]|3[01])$"
        ")"
    )

    # DATETIME / TIMESTAMP format pattern
    datetime_regex = "(" "^\\d{4}[-/]\\d{2}[-/]\\d{2}[ T]\\d{2}:\\d{2}(:\\d{2})?$" ")"

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
            if row["DATA_TYPE"].upper() in ("TEXT", "VARCHAR", "STRING", "CHAR")
        ]

        date_named_columns = [
            col for col in candidate_columns if "DATE" in col["name"].upper()
        ]

        flagged = set()

        if candidate_columns:
            # Expressions to detect DATE
            date_exprs = [
                count(
                    when(
                        expr(
                            f"LENGTH({col['name']}) BETWEEN 8 AND 10 AND "
                            f"regexp_like({col['name']}, '{date_regex}') AND "
                            f"TRY_TO_DATE({col['name']}) IS NOT NULL"
                        ),
                        col["name"],
                    )
                ).alias(f"date_{col['name']}")
                for col in candidate_columns
            ]

            # Expressions to detect DATETIME
            datetime_exprs = [
                count(
                    when(
                        expr(
                            f"regexp_like({col['name']}, '{datetime_regex}') AND "
                            f"TRY_TO_TIMESTAMP({col['name']}) IS NOT NULL"
                        ),
                        col["name"],
                    )
                ).alias(f"datetime_{col['name']}")
                for col in candidate_columns
            ]

            # Combine both
            df = session.table(full_table_name).agg(*(date_exprs + datetime_exprs))
            df_result = df.collect()[0].as_dict()

            for col in candidate_columns:
                date_count = df_result.get(f"date_{col['name']}", 0)
                dt_count = df_result.get(f"datetime_{col['name']}", 0)

                if dt_count > 0:
                    result_rows.append(
                        (
                            database,
                            schema,
                            table,
                            col["name"],
                            col["datatype"],
                            "TIMESTAMP",
                        )
                    )
                    flagged.add(col["name"])

                elif date_count > 0:
                    result_rows.append(
                        (database, schema, table, col["name"], col["datatype"], "DATE")
                    )
                    flagged.add(col["name"])

        # Fields with "date" in name that weren’t flagged yet
        for col in date_named_columns:
            if col["name"] not in flagged:
                result_rows.append(
                    (
                        database,
                        schema,
                        table,
                        col["name"],
                        col["datatype"],
                        "DATE (by name)",
                    )
                )

    # Final output
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
