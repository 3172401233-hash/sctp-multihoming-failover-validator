# -*- coding: utf-8 -*-
"""模拟器使用的数据结构。

这些结构只描述关联状态机需要的字段，不是真实 SCTP 报文格式。
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional


class PathStatus(Enum):
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"


class AssocState(Enum):
    ESTABLISHED = "ESTABLISHED"
    CLOSED = "CLOSED"


@dataclass
class DataChunk:
    """一个待发送/已发送的 DATA 块（消息的一个分片）。"""

    tsn: int
    msg_id: int
    frag_no: int
    frag_count: int
    size: int
    acked: bool = False
    sends: int = 0
    last_path: Optional[str] = None
    gap_reports: int = 0


@dataclass
class Transmission:
    """一次实际发送记录：哪个 TSN、走了哪条路径、是否重传。"""

    time: int
    tsn: int
    path: str
    retrans: bool


@dataclass
class Sack:
    """选择性确认：累积 TSN、间隙块（已收到的更大 TSN）、重复 TSN 报告。"""

    cum_tsn: int
    gaps: List[int] = field(default_factory=list)
    dup_tsns: List[int] = field(default_factory=list)
