"""FLO customer RFM analysis."""

from flo_rfm.campaigns import build_campaign_targets
from flo_rfm.data import data_prep, load_data, prepare_data
from flo_rfm.pipeline import run_analysis
from flo_rfm.rfm import create_rfm

__all__ = [
    "build_campaign_targets",
    "create_rfm",
    "data_prep",
    "load_data",
    "prepare_data",
    "run_analysis",
]
