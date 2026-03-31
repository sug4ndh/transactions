from config import FeatureConfig, PipelineConfig

def test_valid_credit_debit_codes():
    """C and D are the only accepted credit/debit codes."""
    cfg = FeatureConfig()
    assert cfg.credit_code == "C"
    assert cfg.debit_code == "D"


def test_window_days_boundary():
    """window_days of exactly 1 is valid — single day window."""
    cfg = FeatureConfig(window_days=1)
    assert cfg.window_days == 1


def test_ref_dates_accepts_multiple_dates():
    """PipelineConfig accepts a list with more than 3 ref dates."""
    cfg = PipelineConfig(ref_dates=["2025-06-28", "2025-06-29", "2025-06-30", "2025-07-01"])
    assert len(cfg.ref_dates) == 4


def test_ref_dates_single_entry_is_valid():
    """A single ref date is valid."""
    cfg = PipelineConfig(ref_dates=["2025-06-30"])
    assert cfg.ref_dates == ["2025-06-30"]
