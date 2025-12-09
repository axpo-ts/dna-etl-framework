"""Silver layer data models.

This module contains table definitions for the silver layer of the medallion architecture.
Silver tables contain cleansed, validated, and enriched data ready for analytics.
"""

from __future__ import annotations

from data_platform.data_model.silver import aaru
from data_platform.data_model.silver import advanced_analytics_platform
from data_platform.data_model.silver import axbal
from data_platform.data_model.silver import ckw
from data_platform.data_model.silver import ebx
from data_platform.data_model.silver import elia
from data_platform.data_model.silver import elviz_no
from data_platform.data_model.silver import endur
from data_platform.data_model.silver import entsoe
from data_platform.data_model.silver import generis
from data_platform.data_model.silver import geocat
from data_platform.data_model.silver import jao
from data_platform.data_model.silver import mds
from data_platform.data_model.silver import meteologica
from data_platform.data_model.silver import meteomatics
from data_platform.data_model.silver import montel
from data_platform.data_model.silver import nena
from data_platform.data_model.silver import neuron
from data_platform.data_model.silver import nordpool
from data_platform.data_model.silver import orca_fr
from data_platform.data_model.silver import reference
from data_platform.data_model.silver import regelleistung
from data_platform.data_model.silver import rte
from data_platform.data_model.silver import swissgrid
from data_platform.data_model.silver import tennet
from data_platform.data_model.silver import valtaa
from data_platform.data_model.silver import volue
from data_platform.data_model.silver import volue_ems
from data_platform.data_model.silver import workflowmanager

__all__ = [
    "aaru",
    "advanced_analytics_platform",
    "axbal",
    "ckw",
    "ebx",
    "elia",
    "elviz_no",
    "endur",
    "entsoe",
    "generis",
    "geocat",
    "jao",
    "mds",
    "meteologica",
    "meteomatics",
    "montel",
    "nena",
    "neuron",
    "nordpool",
    "orca_fr",
    "reference",
    "regelleistung",
    "rte",
    "swissgrid",
    "tennet",
    "valtaa",
    "volue",
    "volue_ems",
    "workflowmanager",
]
