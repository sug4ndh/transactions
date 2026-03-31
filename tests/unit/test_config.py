import pytest
from config import Config, FeatureConfig, PipelineConfig
from dataclasses import FrozenInstanceError


def test_default_config_is_valid():
    """Config initialises without errors using default values."""
    cfg = Config()
    assert cfg.pipeline.app_name == "transaction_feature_pipeline"
    assert cfg.features.window_days == 7
    assert len(cfg.pipeline.ref_dates) == 3

def test_config_is_immutable():
    cfg = Config()
    with pytest.raises(FrozenInstanceError):
        cfg.features.window_days = 14

def test_window_days_must_be_positive():
    with pytest.raises(ValueError):
        FeatureConfig(window_days=0)
    with pytest.raises(ValueError):
        FeatureConfig(window_days=-1)

def test_ref_dates_must_not_be_empty():
    with pytest.raises(ValueError):
        PipelineConfig(ref_dates=[])


def test_feature_codes_are_correct():
    """Credit and debit codes match expected values."""
    cfg = Config()
    assert cfg.features.credit_code == "C"
    assert cfg.features.debit_code == "D"


def test_transaction_types_are_lowercase():
    """Wire and card type strings are lowercase for consistent matching."""
    cfg = Config()
    assert cfg.features.wire_type == cfg.features.wire_type.lower()
    assert cfg.features.card_type == cfg.features.card_type.lower()
