import snowflake.snowpark as snowpark
from snowflake.snowpark.types import StructType, StructField, StringType, IntegerType


def main(session: snowpark.Session):
    """
    Fetches object type and recursively retrieves full lineage for tables, views, dynamic tables, or stages.
    Returns a Snowpark DataFrame with additional tracking for input database, schema, and object.
    """
    input_objects = [
        ("Database_name", "Schema_name", "Object_name"),
    ]

    all_lineage_dfs = []

    for input_database, input_schema, input_object in input_objects:
        session.sql(
            f"USE DATABASE {input_database}"
        ).collect()  # ✅ Ensure correct database context

        # Identify object type
        object_type = get_object_type(
            session, input_database, input_schema, input_object
        )

        # Map object type to correct domain
        if object_type in ["BASE TABLE", "DYNAMIC TABLE", "VIEW"]:
            object_domain = "TABLE"
        elif object_type == "STAGE":
            object_domain = "STAGE"
        else:
            print(f"Unknown object type: {object_type}")
            continue  # Skip unknown objects

        # Define lineage parameters
        direction = (
            "UPSTREAM"  # 'UPSTREAM' for dependencies, 'DOWNSTREAM' for affected objects
        )
        max_distance = 5  # Max lineage depth (default 5, max 5)

        # Fetch full recursive lineage for each input object
        lineage_df = get_full_lineage(
            session,
            input_database,
            input_schema,
            input_object,
            object_domain,
            direction,
            max_distance,
        )

        if lineage_df.count() > 0:
            all_lineage_dfs.append(lineage_df)

    # Merge all DataFrames into a single result
    if all_lineage_dfs:
        final_df = all_lineage_dfs[0]
        for df in all_lineage_dfs[1:]:
            final_df = final_df.union_all(df)
        return final_df

    return create_empty_dataframe(session)  # ✅ Return empty DataFrame if no results


def get_object_type(session, database_name, schema_name, object_name):
    """
    Determines whether the object is a table, view, dynamic table, or stage.
    """
    table_query = f"""
        SELECT TABLE_TYPE 
        FROM {database_name}.INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = '{schema_name}' 
        AND TABLE_NAME = '{object_name}'
    """

    table_df = session.sql(table_query).collect()

    if table_df:
        return table_df[0]["TABLE_TYPE"]

    view_query = f"""
        SELECT 'VIEW' AS OBJECT_TYPE
        FROM {database_name}.INFORMATION_SCHEMA.VIEWS 
        WHERE TABLE_SCHEMA = '{schema_name}' 
        AND TABLE_NAME = '{object_name}'
    """

    view_df = session.sql(view_query).collect()

    if view_df:
        return "VIEW"

    stage_query = f"""
        SELECT 'STAGE' AS OBJECT_TYPE
        FROM {database_name}.INFORMATION_SCHEMA.STAGES
        WHERE STAGE_SCHEMA = '{schema_name}' 
        AND STAGE_NAME = '{object_name}'
    """

    stage_df = session.sql(stage_query).collect()

    if stage_df:
        return "STAGE"

    return "UNKNOWN"


def get_full_lineage(
    session,
    input_database,
    input_schema,
    input_object,
    object_domain,
    direction,
    max_distance,
):
    """
    Recursively fetches full lineage while tracking the input object details.
    """
    all_lineage_data = []  # Store all lineage data
    processed_objects = set()  # Track processed objects to avoid duplication

    def expand_lineage(last_lineage_df, current_distance):
        """
        Expand lineage by recursively checking last distance and fetching further dependencies.
        """
        if last_lineage_df is None or last_lineage_df.count() == 0:
            return  # Stop if no further dependencies exist

        # Store new lineage
        lineage_list = last_lineage_df.collect()
        all_lineage_data.extend(lineage_list)

        # Get the max distance in the last lineage
        max_last_distance = max(row["DISTANCE"] for row in lineage_list)

        # Extract new targets for next iteration
        next_objects = {
            (
                row["TARGET_OBJECT_DATABASE"],
                row["TARGET_OBJECT_SCHEMA"],
                row["TARGET_OBJECT_NAME"],
            )
            for row in lineage_list
        }

        new_lineage_data = []
        for db, schema, obj in next_objects:
            if (
                db,
                schema,
                obj,
            ) not in processed_objects and max_last_distance + 1 <= max_distance:
                processed_objects.add((db, schema, obj))
                next_lineage_df = get_data_lineage(
                    session,
                    db,
                    schema,
                    obj,
                    object_domain,
                    direction,
                    max_last_distance + 1,
                    input_database,
                    input_schema,
                    input_object,
                )
                if next_lineage_df.count() > 0:
                    new_lineage_data.append(next_lineage_df)

        # If new lineage data exists, recursively expand
        if new_lineage_data:
            merged_df = new_lineage_data[0]
            for df in new_lineage_data[1:]:
                merged_df = merged_df.union_all(df)

            expand_lineage(merged_df, max_last_distance + 1)

    # Initial fetch for input object
    initial_lineage_df = get_data_lineage(
        session,
        input_database,
        input_schema,
        input_object,
        object_domain,
        direction,
        1,
        input_database,
        input_schema,
        input_object,
    )

    # Add input object as distance = 0
    input_object_df = session.create_dataframe(
        [
            [
                0,
                object_domain,
                input_database,
                input_schema,
                input_object,
                "ACTIVE",
                None,
                None,
                None,
                None,
                None,
                input_database,
                input_schema,
                input_object,
            ]
        ],
        schema=get_dataframe_schema(),
    )

    # Start expanding lineage
    expand_lineage(initial_lineage_df, 1)

    # Convert all stored lineage into DataFrame
    if not all_lineage_data:
        return create_empty_dataframe(session)

    final_lineage_df = session.create_dataframe(
        all_lineage_data, schema=get_dataframe_schema()
    )

    # Combine input object with full lineage
    return input_object_df.union_all(final_lineage_df)


def get_data_lineage(
    session,
    database_name,
    schema_name,
    object_name,
    object_domain,
    direction,
    distance,
    input_database,
    input_schema,
    input_object,
):
    """
    Runs the SNOWFLAKE.CORE.GET_LINEAGE function to retrieve data lineage.
    Includes tracking of input database, schema, and object.
    """
    fully_qualified_name = f"{database_name}.{schema_name}.{object_name}"

    query = f"""
        SELECT 
            {distance} AS DISTANCE, -- ✅ Ensures distance is increased dynamically
            SOURCE_OBJECT_DOMAIN, SOURCE_OBJECT_DATABASE, SOURCE_OBJECT_SCHEMA, SOURCE_OBJECT_NAME, SOURCE_STATUS,
            TARGET_OBJECT_DOMAIN, TARGET_OBJECT_DATABASE, TARGET_OBJECT_SCHEMA, TARGET_OBJECT_NAME, TARGET_STATUS,
            '{input_database}' AS INPUT_DATABASE, '{input_schema}' AS INPUT_SCHEMA, '{input_object}' AS INPUT_OBJECT_NAME
        FROM TABLE(
            SNOWFLAKE.CORE.GET_LINEAGE(
                '{fully_qualified_name}', 
                '{object_domain}', 
                '{direction}', 
                {distance}
            )
        )
    """

    return session.sql(query)  # ✅ Ensures returning a Snowpark DataFrame


def create_empty_dataframe(session):
    """
    Creates an empty Snowpark DataFrame with a predefined schema.
    """
    schema = get_dataframe_schema()
    return session.create_dataframe(
        [], schema=schema
    )  # ✅ Returns empty DataFrame with schema


def get_dataframe_schema():
    """
    Defines the schema for the lineage DataFrame with tracking for input database, schema, and object.
    """
    return StructType(
        [
            StructField("DISTANCE", IntegerType(), True),
            StructField("SOURCE_OBJECT_DOMAIN", StringType(), True),
            StructField("SOURCE_OBJECT_DATABASE", StringType(), True),
            StructField("SOURCE_OBJECT_SCHEMA", StringType(), True),
            StructField("SOURCE_OBJECT_NAME", StringType(), True),
            StructField("SOURCE_STATUS", StringType(), True),
            StructField("TARGET_OBJECT_DOMAIN", StringType(), True),
            StructField("TARGET_OBJECT_DATABASE", StringType(), True),
            StructField("TARGET_OBJECT_SCHEMA", StringType(), True),
            StructField("TARGET_OBJECT_NAME", StringType(), True),
            StructField("TARGET_STATUS", StringType(), True),
            StructField("INPUT_DATABASE", StringType(), True),
            StructField("INPUT_SCHEMA", StringType(), True),
            StructField("INPUT_OBJECT_NAME", StringType(), True),
        ]
    )
