from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment, DataTypes
from pyflink.table.udf import udf
from pyflink.table.expressions import col, call
import os


# Step 1: Define the UDF
@udf(result_type=DataTypes.INT())
def count_urls(url_list: str) -> int:
    """
    Splits a comma-separated string and returns the number of elements.
    """
    if not url_list:
        return 0
    return len(url_list.split(","))


def create_processed_sessionized_source_kafka(t_env):
    table_name = "sessionized_events_kafka"
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            host VARCHAR,
            urls VARCHAR,
            session_start TIMESTAMP(3),
            WATERMARK FOR session_start AS session_start - INTERVAL '15' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = '{os.environ.get('KAFKA_URL')}',
            'topic' = '{os.environ.get('KAFKA_TOPIC')}',
            'properties.group.id' = '{os.environ.get('KAFKA_GROUP')}',
            'format' = 'json'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def create_avg_events_sink_postgres(t_env):
    table_name = 'avg_events_per_session'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            host VARCHAR,
            avg_events_per_session DOUBLE,
            PRIMARY KEY (host) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def log_aggregation():
    # Step 2: Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # Step 3: Register the UDF
    t_env.create_temporary_function("count_urls", count_urls)

    # Create source and sink tables
    source_table = create_processed_sessionized_source_kafka(t_env)
    sink_table = create_avg_events_sink_postgres(t_env)

    # Step 4: Query to calculate average events per session for techcreator.io
    t_env.from_path(source_table) \
        .filter(col("host").like("%techcreator.io")) \
        .select(
            col("host"),
            # Use the custom UDF to count the number of URLs
            call("count_urls", col("urls")).alias("num_urls")
        ) \
        .group_by(col("host")) \
        .select(
            col("host"),
            col("num_urls").avg.alias("avg_events_per_session")
        ) \
        .execute_insert(sink_table)


if __name__ == '__main__':
    log_aggregation()