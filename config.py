from dataclasses import dataclass, field
from typing import List


@dataclass(frozen=True)
class PathConfig:
    transactions: str = "s3://input-transactions/data/Transactions.csv"
    transaction_types: str = "s3://input-transactions/data/TransactionType.csv"
    output_dir: str = "s3://input-transactions/output"
    output_filename: str = "features_output.csv"


@dataclass(frozen=True)
class SchemaConfig:
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


@dataclass(frozen=True)
class FeatureConfig:
    wire_type: str = "wire"
    card_type: str = "card"
    credit_code: str = "C"
    debit_code: str = "D"
    window_days: int = 7
    def __post_init__(self):
        if self.window_days <= 0:
            raise ValueError("window_days must be a positive integer")


@dataclass(frozen=True)
class PipelineConfig:
    ref_dates: List[str] = field(default_factory=lambda: [
        "2025-06-28",
        "2025-06-29",
        "2025-06-30",
    ])
    app_name: str = "transaction_feature_pipeline"
    log_level: str = "ERROR"

    def __post_init__(self):
        if not self.ref_dates:
            raise ValueError("ref_dates cannot be empty")

@dataclass(frozen=True)
class Config:
    paths: PathConfig = field(default_factory=PathConfig)
    schema_: SchemaConfig = field(default_factory=SchemaConfig)
    features: FeatureConfig = field(default_factory=FeatureConfig)
    pipeline: PipelineConfig = field(default_factory=PipelineConfig)


config = Config()
