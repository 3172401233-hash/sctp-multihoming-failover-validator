# -*- coding: utf-8 -*-
"""SCTP 多宿主故障切换验证器（关联状态模拟，不构造真实 SCTP 报文）。"""

from .association import Association
from .model import AssocState, DataChunk, PathStatus, Sack, Transmission

__all__ = [
    "Association",
    "AssocState",
    "DataChunk",
    "PathStatus",
    "Sack",
    "Transmission",
]

__version__ = "0.1.0"
