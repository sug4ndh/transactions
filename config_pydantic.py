from pydantic import BaseModel, field_validator
from typing import List


class PathConfig(BaseModel):
    transactions: str = "s3://input-transactions/data/Transactions.csv"
    transaction_types: str = "s3://input-transactions/data/TransactionType.csv"
    output_dir: str = "s3://input-transactions/output"
    output_filename: str = "features_output.csv"
    model_config = {"frozen": True} 


class SchemaConfig(BaseModel):
    customer_id: str = "customer_id"
    transaction_id: str = "transaction_id"
    transaction_date: str = "transaction_date"
    credit_debit_code: str = "credit_debit_code"
    transaction_type_code: str = "transaction_type_code"
    transaction_value: str = "transaction_value"
    transaction_type: str = "transaction_type"
    ref_date: str = "ref_date"
    feature_1: str = "feature_1"
    feature_2: str = "feature_2"
    feature_3: str = "feature_3"
    model_config = {"frozen": True} 


class FeatureConfig(BaseModel):
    wire_type: str = "wire"
    card_type: str = "card"
    credit_code: str = "C"
    debit_code: str = "D"
    window_days: int = 7

    @field_validator("window_days")
    @classmethod
    def window_must_be_positive(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("window_days must be a positive integer")
        return v
    model_config = {"frozen": True} 

class PipelineConfig(BaseModel):
    ref_dates: List[str] = ["2025-06-28", "2025-06-29", "2025-06-30"]
    app_name: str = "transaction_feature_pipeline"
    log_level: str = "ERROR"

    @field_validator("ref_dates")
    @classmethod
    def ref_dates_must_not_be_empty(cls, v: List[str]) -> List[str]:
        if not v:
            raise ValueError("ref_dates cannot be empty")
        return v
    model_config = {"frozen": True} 


class Config(BaseModel):
    paths: PathConfig = PathConfig()
    schema_: SchemaConfig = SchemaConfig()
    features: FeatureConfig = FeatureConfig()
    pipeline: PipelineConfig = PipelineConfig()

    model_config = {"frozen": True}


config = Config()
