from datetime import datetime
from typing import List

from dagster import (
    AssetSelection,
    Nothing,
    OpExecutionContext,
    ScheduleDefinition,
    String,
    asset,
    define_asset_job,
    load_assets_from_current_module,
)
from workspaces.types import Aggregation, Stock


@asset(
    config_schema={"s3_key": String},
    required_resource_keys={"s3"},
    description="Get a list of stocks from an S3 file.",
    op_tags={"kind": "s3"},
)
def get_s3_data(context):
    s3_key = context.op_config["s3_key"]
    s3_data = context.resources.s3.get_data(s3_key)
    stocks = list(Stock.from_list(r) for r in s3_data)
    return stocks


@asset(
    description="Given a list of stocks return an Aggregation with the highest value stock.",
)
def process_data(context, get_s3_data):
    stocks = get_s3_data
    highest_value_stock = max(stocks, key = lambda k: k.high)
    result = Aggregation(date=highest_value_stock.date, high=highest_value_stock.high)
    return result


@asset(
    description="Upload an Aggregation to Redis.",
    required_resource_keys={"redis"},
    op_tags={"kind": "redis"},       
)
def put_redis_data(context, process_data):
    aggregation = process_data
    context.resources.redis.put_data(str(aggregation.date), str(aggregation.high))


@asset(
    description="Upload an Aggregation to an S3 file.",
    required_resource_keys={"s3"},
    op_tags={"kind": "s3"},
)
def put_s3_data(context, process_data):
    aggregation = process_data
    d=datetime.utcnow().strftime('%Y-%m-%d')
    key_name=f'/aggregation_{d}.csv'
    context.resources.s3.put_data(key_name, aggregation)


project_assets = load_assets_from_current_module()


machine_learning_asset_job = define_asset_job(
    name="machine_learning_asset_job",
)

machine_learning_schedule = ScheduleDefinition(job=machine_learning_asset_job, cron_schedule="*/15 * * * *")
