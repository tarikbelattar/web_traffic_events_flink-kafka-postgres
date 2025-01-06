from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table.udf import ScalarFunction, udf
import os
import json
import requests
from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, StreamTableEnvironment

def create_processed_events_sink_kafka(t_env): 
    table_name = "sessionized_events_kafka"
    kafka_key = os.environ.get("KAFKA_WEB_TRAFFIC_KEY", "")
    kafka_secret = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET", "")
    sasl_config = f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_key}" password="{kafka_secret}";'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
             ip VARCHAR,
            host VARCHAR,
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            urls VARCHAR
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = '{os.environ.get('KAFKA_URL')}',
            'topic' = '{os.environ.get('KAFKA_GROUP').split('.')[0] + '.' + table_name}',
            'properties.ssl.endpoint.identification.algorithm' = '',
            'properties.group.id' = '{os.environ.get('KAFKA_GROUP')}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.jaas.config' = '{sasl_config}',
            'format' = 'json'
        );
        """
    print(sink_ddl)
    t_env.execute_sql(sink_ddl)
    return table_name
def create_sessionized_events_sink_postgres(t_env):  
    table_name = 'sessionized_events'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,
            host VARCHAR,
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            urls VARCHAR
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

def create_sessionized_events_source_kafka(t_env):
    kafka_key = os.environ.get("KAFKA_WEB_TRAFFIC_KEY", "")
    kafka_secret = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET", "")
    table_name = "events"
    pattern = "yyyy-MM-dd''T''HH:mm:ss.SSS''Z''"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            url VARCHAR,
            referrer VARCHAR,
            user_agent VARCHAR,
            host VARCHAR,
            ip VARCHAR,
            headers VARCHAR,
            event_time VARCHAR,
            event_timestamp AS TO_TIMESTAMP(event_time, '{pattern}'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '15' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = '{os.environ.get('KAFKA_URL')}',
            'topic' = '{os.environ.get('KAFKA_TOPIC')}',
            'properties.group.id' = '{os.environ.get('KAFKA_GROUP')}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{kafka_key}\" password=\"{kafka_secret}\";',
            'scan.startup.mode' = 'latest-offset',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'json'
        );
        """
    print(source_ddl)
    t_env.execute_sql(source_ddl)
    return table_name

def log_processing():
    print('Starting Job!')
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    print('got streaming environment')
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(3)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    try:
        # Create Kafka table
        source_table = create_sessionized_events_source_kafka(t_env)
        postgres_sink = create_sessionized_events_sink_postgres(t_env)
        print('loading into postgres')
        t_env.execute_sql(
            f"""
                    INSERT INTO {postgres_sink} 
                    SELECT
                        ip,
                        host,
                        TUMBLE_START(event_timestamp, INTERVAL '5' MINUTES) AS session_start,
                        TUMBLE_END(event_timestamp, INTERVAL '5' MINUTES) AS session_end,
                        CAST(COLLECT(url) AS STRING) AS urls
                    FROM {source_table}
                    GROUP BY TUMBLE(event_timestamp, INTERVAL '5' MINUTES), ip, host
                    """
        ).wait()
    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == '__main__':
    log_processing()
