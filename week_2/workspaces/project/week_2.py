from datetime import datetime
from typing import List

from dagster import (
    In,
    Nothing,
    OpExecutionContext,
    Out,
    ResourceDefinition,
    String,
    graph,
    op,
)
from workspaces.config import REDIS, S3, S3_FILE
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
    "ops": {
        "get_s3_data": {
            "config": {
                "s3_key": S3_FILE
            }
        }
    }
}

docker = {
    "resources": {
        "s3": {
            "config": S3
        },
        "redis": {
            "config": REDIS
        }
    },
    "ops": {
        "get_s3_data": {
            "config": {
                "s3_key": S3_FILE
            }
        }
    }
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
    }
)
