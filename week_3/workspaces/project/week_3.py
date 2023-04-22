from datetime import datetime
from typing import List

from dagster import (
    In,
    Nothing,
    OpExecutionContext,
    Out,
    ResourceDefinition,
    RetryPolicy,
    RunRequest,
    ScheduleDefinition,
    SensorEvaluationContext,
    SkipReason,
    String,
    graph,
    op,
    schedule,
    sensor,
    static_partitioned_config,
)
from workspaces.config import REDIS, S3
from workspaces.project.sensors import get_s3_keys
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@op(
    config_schema={"s3_key": String},
    required_resource_keys={"s3"},
    description="Get a list of stocks from an S3 file.",
    out={"stocks": Out(dagster_type=List[Stock], description="Stock list")},
    tags={"kind": "s3"},
)
def get_s3_data(context: OpExecutionContext) -> List[Stock]:
    s3_key = context.op_config["s3_key"]
    s3_data = context.resources.s3.get_data(s3_key)
    stocks = list(Stock.from_list(r) for r in s3_data)
    return stocks

@op(
    description="Given a list of stocks return an Aggregation with the highest value stock.",
    ins={"stocks": In(dagster_type=List[Stock], description="Stock list")},
    out={"aggregation": Out(dagster_type=Aggregation, description="Highest value stock")},
)
def process_data(context: OpExecutionContext, stocks: List[Stock]) -> Aggregation:
    highest_value_stock = max(stocks, key = lambda k: k.high)
    result = Aggregation(date=highest_value_stock.date, high=highest_value_stock.high)
    return result

@op(
    description="Upload an Aggregation to Redis.",
    ins={"aggregation": In(dagster_type=Aggregation, description="Highest value stock")},
    out=Out(Nothing),
    required_resource_keys={"redis"},
    tags={"kind": "redis"},
)
def put_redis_data(context: OpExecutionContext, aggregation: Aggregation):
    context.resources.redis.put_data(str(aggregation.date), str(aggregation.high))

@op(
    description="Upload an Aggregation to an S3 file.",
    ins={"aggregation": In(dagster_type=Aggregation, description="Highest value stock")},
    out=Out(Nothing),
    required_resource_keys={"s3"},
    tags={"kind": "s3"},
)
def put_s3_data(context: OpExecutionContext, aggregation: Aggregation):
    d=datetime.utcnow().strftime('%Y-%m-%d')
    key_name=f'/aggregation_{d}.csv'
    context.resources.s3.put_data(key_name, aggregation)

@graph
def machine_learning_graph():
    stocks = get_s3_data()
    result = process_data(stocks)
    put_redis_data(result)
    put_s3_data(result)

local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}}
}

docker = {
    "resources": {
        "s3": {"config": S3},
        "redis": {"config": REDIS}
    },
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}}
}

@static_partitioned_config(partition_keys=[str(n) for n in range(1, 11)])
def docker_config(partition_key: int):
    return {
        "resources": {
            "s3": {"config": S3},
            "redis": {"config": REDIS}
        },
        "ops": {"get_s3_data": {"config": {"s3_key": f"prefix/stock_{partition_key}.csv"}}}
    }

machine_learning_job_local = machine_learning_graph.to_job(
    name="machine_learning_job_local",
    config=local,
    resource_defs={
        "s3":mock_s3_resource,
        "redis":ResourceDefinition.mock_resource()
    }
)

machine_learning_job_docker = machine_learning_graph.to_job(
    name="machine_learning_job_docker",
    config=docker,
    resource_defs={
        "s3": s3_resource,
        "redis":redis_resource
    },
    op_retry_policy=RetryPolicy(max_retries=10, delay=1)
)

machine_learning_schedule_local = ScheduleDefinition(
    job=machine_learning_job_local,
    cron_schedule="*/15 * * * *"
)

@schedule(
    job=machine_learning_job_docker,
    cron_schedule="0 * * * *"
)
def machine_learning_schedule_docker():
    for pk in docker_config.get_partition_keys():
        yield RunRequest(
            run_key=pk,
            run_config=docker_config.get_run_config_for_partition_key(pk)
        )

@sensor(
    job=machine_learning_job_docker,
    minimum_interval_seconds=30
)
def machine_learning_sensor_docker(context):
    new_s3_keys = get_s3_keys(bucket="dagster",
                              prefix="prefix",
                              endpoint_url="http://localstack:4566")
    if not new_s3_keys:
        yield SkipReason("No new s3 files found in bucket.")
        return 

    for key in new_s3_keys:
        config = {
            "resources": {
                "s3": {"config": S3},
                "redis": {"config": REDIS}
            },
            "ops": {"get_s3_data": {"config": {"s3_key": key}}}
        }
        yield RunRequest(run_key=key, run_config=config)
