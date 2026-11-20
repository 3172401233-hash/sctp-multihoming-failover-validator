# -*- coding: utf-8 -*-
"""SCTP 多宿主关联状态模拟核心。

只模拟关联/路径状态机、定时器与错误计数器语义（RFC 4960 风格）：

* 每条路径独立的 HEARTBEAT 定时器与错误计数；
* 数据重传使用独立的 RTO 定时器（指数退避，封顶）；
* 路径错误计数超过 path_max_retrans -> 路径 INACTIVE -> 触发故障切换；
* 关联错误计数超过 assoc_max_retrans -> 关联 CLOSED;
* 心跳成功只清零错误计数，不代表数据路径可用，也不确认任何数据；
* 恢复的路径转为 ACTIVE，但不会立刻替代仍有效的当前数据路径。

不构造、解析任何真实 SCTP 报文。
"""

from collections import deque

from .model import AssocState, DataChunk, PathStatus, Sack, Transmission


class Path(object):
    """一条通向对端的网络路径。"""

    def __init__(self, name, address, hb_interval, max_retrans):
        self.name = name
        self.address = address
        self.status = PathStatus.ACTIVE
        self.link_up = True
        self.error_count = 0
        self.max_retrans = max_retrans
        self.hb_interval = hb_interval
        self.hb_timer = hb_interval
        self.hb_sent = 0
        self.hb_acked = 0

    def state_dict(self):
        return {
            "address": self.address,
            "status": self.status.value,
            "link_up": self.link_up,
            "error_count": self.error_count,
            "hb_sent": self.hb_sent,
            "hb_acked": self.hb_acked,
        }


class _Peer(object):
    """简化的对端接收模型：记录收到的 TSN，生成 SACK，报告重复 TSN。"""

    def __init__(self):
        self.received = set()
        self.cum = 0

    def receive(self, tsn):
        dups = []
        if tsn in self.received:
            dups.append(tsn)
        else:
            self.received.add(tsn)
        while self.cum + 1 in self.received:
            self.cum += 1
        gaps = sorted(t for t in self.received if t > self.cum)
        return Sack(cum_tsn=self.cum, gaps=gaps, dup_tsns=dups)


class Association(object):
    """一个 SCTP 关联的发送端状态机模拟器。"""

    def __init__(self, rto_initial=3, rto_max=24, assoc_max_retrans=10,
                 path_max_retrans=5, hb_interval=6,
                 fast_retransmit_threshold=3):
        self.state = AssocState.ESTABLISHED
        self.time = 0
        self.paths = {}
        self.path_order = []
        self.primary_path = None
        self.current_path = None
        # RTO 定时器（数据重传用，与心跳定时器相互独立）
        self.rto_initial = rto_initial
        self.rto_max = rto_max
        self.rto = rto_initial
        self.rto_timer = None
        self.assoc_max_retrans = assoc_max_retrans
        self.path_max_retrans = path_max_retrans
        self.assoc_error_count = 0
        self.hb_interval = hb_interval
        self.fast_retransmit_threshold = fast_retransmit_threshold
        self.next_tsn = 1
        self.send_queue = deque()
        self.flight = {}  # tsn -> 已发送未确认的 DataChunk
        self.cum_tsn_acked = 0
        # 置 True 模拟 SACK 丢失（对端收到数据但确认没有回来）
        self.drop_sacks = False
        self.transmissions = []
        self.events = []
        self.dup_tsns = []
        self._peer = _Peer()
        self._pending = []  # (deliver_at, kind, payload)

    # ---------------------------------------------------------------- 配置

    def add_path(self, name, address, primary=False):
        path = Path(name, address, self.hb_interval, self.path_max_retrans)
        self.paths[name] = path
        self.path_order.append(name)
        if primary or self.primary_path is None:
            self.primary_path = name
        if primary or self.current_path is None:
            self.current_path = name
        self._log("path-added", path=name, address=address, primary=primary)
        return path

    def set_link(self, name, up):
        """模拟底层链路通断。

        链路断开不会立刻改变路径状态；
        路径状态只能由心跳/RTO 错误计数驱动。
        """
        self.paths[name].link_up = up
        self._log("link-up" if up else "link-down", path=name)

    # --------------------------------------------------------------- 发送侧

    def send_message(self, msg_id, size, fragments=1):
        """把一条消息切成 fragments 个 DATA 块放入发送队列并尽量发送。"""
        if self.state is AssocState.CLOSED:
            self._log("send-rejected-closed", msg_id=msg_id)
            return []
        tsns = []
        for i in range(fragments):
            chunk = DataChunk(tsn=self.next_tsn, msg_id=msg_id,
                              frag_no=i + 1, frag_count=fragments, size=size)
            self.next_tsn += 1
            self.send_queue.append(chunk)
            tsns.append(chunk.tsn)
        self._log("message-queued", msg_id=msg_id, tsns=list(tsns))
        self._pump()
        return tsns

    def _pump(self):
        while self.send_queue:
            if not self._ensure_current_path():
                self._log("send-deferred-no-active-path")
                return
            chunk = self.send_queue.popleft()
            self._transmit(chunk, retrans=False)
            self.flight[chunk.tsn] = chunk

    def _transmit(self, chunk, retrans):
        path = self.paths[self.current_path]
        chunk.sends += 1
        chunk.last_path = path.name
        chunk.gap_reports = 0
        self.transmissions.append(
            Transmission(self.time, chunk.tsn, path.name, retrans))
        self._log("transmit", tsn=chunk.tsn, path=path.name, retrans=retrans)
        if self.rto_timer is None:
            self.rto_timer = self.rto
        if path.link_up:
            self._pending.append((self.time + 1, "data",
                                  (chunk.tsn, path.name)))
        # 链路已断：块发出去就丢了，只能靠 RTO 发现

    # --------------------------------------------------------------- 定时器

    def tick(self, n=1):
        """推进 n 个离散时间片。"""
        for _ in range(n):
            if self.state is AssocState.CLOSED:
                return
            self._tick_once()

    def _tick_once(self):
        self.time += 1
        # 1) 各路径心跳定时器（与 RTO 定时器完全独立）
        for path in self.paths.values():
            path.hb_timer -= 1
            if path.hb_timer <= 0:
                path.hb_timer = self.hb_interval
                self._send_heartbeat(path)
        if self.state is AssocState.CLOSED:
            return
        # 2) RTO 定时器
        if self.rto_timer is not None and self.flight:
            self.rto_timer -= 1
            if self.rto_timer <= 0:
                self._rto_expired()
        if self.state is AssocState.CLOSED:
            return
        # 3) 到期的网络投递
        due = [item for item in self._pending if item[0] <= self.time]
        self._pending = [item for item in self._pending if item[0] > self.time]
        for _, kind, payload in due:
            if kind == "data":
                tsn, path_name = payload
                self._peer_receive(tsn, path_name)
            elif kind == "sack":
                self._process_sack(payload)
            elif kind == "hb_ack":
                self._heartbeat_ack(payload)

    # ----------------------------------------------------------------- 心跳

    def _send_heartbeat(self, path):
        path.hb_sent += 1
        self._log("heartbeat-sent", path=path.name)
        if path.link_up:
            self._pending.append((self.time + 1, "hb_ack", path.name))
        else:
            self._log("heartbeat-timeout", path=path.name)
            self._path_error(path, "heartbeat-timeout")

    def _heartbeat_ack(self, path_name):
        path = self.paths[path_name]
        path.hb_acked += 1
        path.error_count = 0
        self.assoc_error_count = 0
        self._log("heartbeat-ack", path=path_name)
        # 心跳成功不代表数据路径可用：不动 flight，也不确认任何数据。
        if path.status is PathStatus.INACTIVE:
            path.status = PathStatus.ACTIVE
            self._log("path-recovered", path=path_name)
            # 恢复的路径不会立刻替代仍有效的当前路径；
            # 仅当当前没有可用路径时才把它选为数据路径。
            if not self._current_usable():
                self._failover()

    # ------------------------------------------------------------- 错误计数

    def _path_error(self, path, cause):
        path.error_count += 1
        self.assoc_error_count += 1
        self._log("path-error", path=path.name, cause=cause,
                  path_errors=path.error_count,
                  assoc_errors=self.assoc_error_count)
        if (path.status is PathStatus.ACTIVE
                and path.error_count > path.max_retrans):
            path.status = PathStatus.INACTIVE
            self._log("path-inactive", path=path.name)
            if self.current_path == path.name:
                self._failover()
        if self.assoc_error_count > self.assoc_max_retrans:
            self._close("assoc-max-retrans-exceeded")

    def _current_usable(self):
        return (self.current_path is not None
                and self.paths[self.current_path].status is PathStatus.ACTIVE)

    def _ensure_current_path(self):
        if self._current_usable():
            return True
        self._failover()
        return self.current_path is not None

    def _failover(self):
        old = self.current_path
        new = None
        for name in self.path_order:
            if self.paths[name].status is PathStatus.ACTIVE:
                new = name
                break
        self.current_path = new
        self._log("failover", old=old, new=new)

    def _close(self, reason):
        self.state = AssocState.CLOSED
        self.rto_timer = None
        self._log("association-closed", reason=reason)

    # ------------------------------------------------------------------ RTO

    def _rto_expired(self):
        oldest = min(self.flight.values(), key=lambda c: c.tsn)
        self._log("rto-expired", path=oldest.last_path, rto=self.rto,
                  unacked=sorted(self.flight))
        # 错误记在最旧未确认块最后一次发送的路径上
        if oldest.last_path is not None:
            self._path_error(self.paths[oldest.last_path], "rto-timeout")
        if self.state is AssocState.CLOSED:
            return
        self.rto = min(self.rto * 2, self.rto_max)
        for chunk in self.flight.values():
            chunk.gap_reports = 0
        # 重传全部未确认块；若原路径已失效，此时应已切换到可用路径
        for chunk in sorted(self.flight.values(), key=lambda c: c.tsn):
            self._retransmit_chunk(chunk)
        self.rto_timer = self.rto if self.flight else None

    def _retransmit_chunk(self, chunk):
        if not self._ensure_current_path():
            self._log("retransmit-deferred-no-active-path", tsn=chunk.tsn)
            return False
        self._transmit(chunk, retrans=True)
        return True

    # ------------------------------------------------------------ SACK 处理

    def inject_sack(self, cum_tsn, gaps=(), dup_tsns=()):
        """测试/脚本直接注入一个 SACK（用于乱序、重复等异常场景）。"""
        self._process_sack(Sack(cum_tsn=cum_tsn, gaps=list(gaps),
                                dup_tsns=list(dup_tsns)))

    def _peer_receive(self, tsn, path_name):
        sack = self._peer.receive(tsn)
        path = self.paths[path_name]
        if self.drop_sacks or not path.link_up:
            self._log("sack-lost", tsn=tsn)
            return
        self._pending.append((self.time + 1, "sack", sack))

    def _process_sack(self, sack):
        if self.state is AssocState.CLOSED:
            return
        self._log("sack", cum=sack.cum_tsn, gaps=list(sack.gaps),
                  dups=list(sack.dup_tsns))
        # 乱序/过期 SACK：累积点只前进不后退
        if sack.cum_tsn < self.cum_tsn_acked:
            self._log("sack-stale-out-of-order", cum=sack.cum_tsn,
                      kept=self.cum_tsn_acked)
        else:
            self.cum_tsn_acked = sack.cum_tsn
        newly_acked = []
        for tsn in sorted(self.flight):
            chunk = self.flight[tsn]
            if tsn <= sack.cum_tsn or tsn in sack.gaps:
                chunk.acked = True
                newly_acked.append(chunk)
        for dup in sack.dup_tsns:
            self.dup_tsns.append(dup)
            self._log("duplicate-tsn-reported", tsn=dup)
        for chunk in newly_acked:
            del self.flight[chunk.tsn]
        if newly_acked:
            self.assoc_error_count = 0
            self.rto = self.rto_initial
            self.rto_timer = self.rto if self.flight else None
        # 快速重传统计：被更晚的 TSN 越过且未确认的块
        max_acked = sack.cum_tsn
        if sack.gaps:
            max_acked = max(max_acked, max(sack.gaps))
        for chunk in self.flight.values():
            if chunk.tsn < max_acked:
                chunk.gap_reports += 1
        self._maybe_fast_retransmit()

    def _maybe_fast_retransmit(self):
        for chunk in sorted(self.flight.values(), key=lambda c: c.tsn):
            if chunk.gap_reports >= self.fast_retransmit_threshold:
                self._log("fast-retransmit", tsn=chunk.tsn,
                          gap_reports=chunk.gap_reports)
                self._retransmit_chunk(chunk)

    # ----------------------------------------------------------------- 输出

    @property
    def unacked_tsns(self):
        return sorted(self.flight)

    def report(self):
        return {
            "time": self.time,
            "association_state": self.state.value,
            "current_path": self.current_path,
            "primary_path": self.primary_path,
            "rto": self.rto,
            "assoc_error_count": self.assoc_error_count,
            "paths": {n: p.state_dict() for n, p in self.paths.items()},
            "unacked_tsns": self.unacked_tsns,
            "send_queue": [c.tsn for c in self.send_queue],
            "duplicate_tsns": list(self.dup_tsns),
            "transmissions": [
                {"time": t.time, "tsn": t.tsn, "path": t.path,
                 "retrans": t.retrans}
                for t in self.transmissions
            ],
            "events": list(self.events),
        }

    def _log(self, event, **fields):
        entry = {"time": self.time, "event": event}
        entry.update(fields)
        self.events.append(entry)
