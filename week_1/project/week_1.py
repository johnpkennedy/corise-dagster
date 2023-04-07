import csv
from datetime import datetime
from typing import Iterator, List

from dagster import (
    graph,
    job,
    op,
    usable_as_dagster_type,
    In,
    Nothing,
    OpExecutionContext,
    Out,
    String,
)
from pydantic import BaseModel


@usable_as_dagster_type(description="Stock data")
class Stock(BaseModel):
    date: datetime
    close: float
    volume: int
    open: float
    high: float
    low: float

    @classmethod
    def from_list(cls, input_list: List[str]):
        """Do not worry about this class method for now"""
        return cls(
            date=datetime.strptime(input_list[0], "%Y/%m/%d"),
            close=float(input_list[1]),
            volume=int(float(input_list[2])),
            open=float(input_list[3]),
            high=float(input_list[4]),
            low=float(input_list[5]),
        )


@usable_as_dagster_type(description="Aggregation of stock data")
class Aggregation(BaseModel):
    date: datetime
    high: float


def csv_helper(file_name: str) -> Iterator[Stock]:
    with open(file_name) as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            yield Stock.from_list(row)


@op(description="Get a list of stocks from S3.",
    config_schema={"s3_key": String})
def get_s3_data_op(context) -> List[Stock]:
    s3_key = context.op_config["s3_key"]
    stocks = list(csv_helper(s3_key))
    return stocks


@op(description="Given a list of stocks return an Aggregation with the highest value.")
def process_data_op(context, stocks: List[Stock]) -> Aggregation:
    highest_value_stock = max(stocks, key = lambda k: k.high)
    result = Aggregation(date=highest_value_stock.date, high=highest_value_stock.high)
    return result


@op(description="Upload an Aggregation to Redis.")
def put_redis_data_op(context, result: Aggregation) -> None:
    pass


@op(description="Upload an Aggregation to an S3 file.")
def put_s3_data_op(context, result: Aggregation) -> None:
    pass


@job
def machine_learning_job():
    stocks = get_s3_data_op()
    result = process_data_op(stocks)
    put_redis_data_op(result)
    put_s3_data_op(result)
