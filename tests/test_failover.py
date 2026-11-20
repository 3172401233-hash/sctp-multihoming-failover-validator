# -*- coding: utf-8 -*-
"""SCTP 多宿主故障切换验证器的单元测试。"""

import unittest

from sctp_sim import Association, AssocState, PathStatus


def make_assoc(**overrides):
    params = dict(rto_initial=2, rto_max=8, assoc_max_retrans=6,
                  path_max_retrans=2, hb_interval=4)
    params.update(overrides)
    assoc = Association(**params)
    assoc.add_path("p1", "10.0.0.1 <-> 192.0.2.1", primary=True)
    assoc.add_path("p2", "10.0.0.2 <-> 192.0.2.2")
    return assoc


def retransmissions(assoc, tsn):
    return [t for t in assoc.transmissions if t.tsn == tsn and t.retrans]


class PrimaryPathDownTest(unittest.TestCase):
    """主路径断开：RTO 超时累积错误，路径 INACTIVE 后切换备用路径重传。"""

    def test_primary_path_down_failover(self):
        assoc = make_assoc()
        assoc.set_link("p1", False)
        assoc.send_message(msg_id=1, size=100)
        self.assertEqual(assoc.transmissions[0].path, "p1")
        self.assertFalse(assoc.transmissions[0].retrans)
        assoc.tick(10)
        self.assertEqual(assoc.paths["p1"].status, PathStatus.INACTIVE)
        self.assertEqual(assoc.current_path, "p2")
        retrans = retransmissions(assoc, tsn=1)
        self.assertTrue(any(t.path == "p2" for t in retrans))
        for t in assoc.transmissions:
            if t.time >= 6:  # p1 在 t=6 被判定 INACTIVE
                self.assertNotEqual(t.path, "p1", "数据不应再发送到已失效路径")
        self.assertEqual(assoc.unacked_tsns, [])
        self.assertEqual(assoc.state, AssocState.ESTABLISHED)


class DualPathFailureTest(unittest.TestCase):
    """双路径同时故障：所有路径 INACTIVE，关联错误计数超限后 CLOSED。"""

    def test_dual_path_failure_closes_association(self):
        assoc = make_assoc()
        assoc.set_link("p1", False)
        assoc.set_link("p2", False)
        assoc.send_message(msg_id=1, size=100)
        assoc.tick(15)
        self.assertEqual(assoc.paths["p1"].status, PathStatus.INACTIVE)
        self.assertEqual(assoc.paths["p2"].status, PathStatus.INACTIVE)
        self.assertEqual(assoc.state, AssocState.CLOSED)
        self.assertEqual(assoc.unacked_tsns, [1])
        self.assertTrue(any(e["event"] == "association-closed"
                            for e in assoc.events))

    def test_simultaneous_failure_then_one_recovers(self):
        assoc = make_assoc(assoc_max_retrans=10)
        assoc.set_link("p1", False)
        assoc.set_link("p2", False)
        assoc.send_message(msg_id=1, size=100)
        assoc.tick(13)
        self.assertEqual(assoc.paths["p1"].status, PathStatus.INACTIVE)
        self.assertEqual(assoc.paths["p2"].status, PathStatus.INACTIVE)
        self.assertIsNone(assoc.current_path)
        self.assertEqual(assoc.unacked_tsns, [1])
        assoc.tick(1)  # t=14：RTO 再次超时，但已没有可用路径
        self.assertTrue(any(e["event"] == "retransmit-deferred-no-active-path"
                            for e in assoc.events))
        assoc.set_link("p2", True)
        assoc.tick(16)
        self.assertEqual(assoc.current_path, "p2")
        self.assertEqual(assoc.unacked_tsns, [])
        self.assertEqual(assoc.state, AssocState.ESTABLISHED)


class PathRecoveryTest(unittest.TestCase):
    """路径恢复：恢复的路径不立刻替代仍有效的当前路径；主路径抖动不回切。"""

    def test_recovery_does_not_replace_current_path(self):
        assoc = make_assoc()
        assoc.set_link("p1", False)
        assoc.send_message(msg_id=1, size=100)
        assoc.tick(8)
        self.assertEqual(assoc.current_path, "p2")
        assoc.set_link("p1", True)
        assoc.tick(6)
        self.assertEqual(assoc.paths["p1"].status, PathStatus.ACTIVE)
        self.assertEqual(assoc.current_path, "p2", "恢复不应立刻回切")
        assoc.send_message(msg_id=2, size=100)
        self.assertEqual(assoc.transmissions[-1].path, "p2")
        assoc.tick(3)
        self.assertEqual(assoc.unacked_tsns, [])
        for _ in range(3):  # 主路径抖动：反复断/通
            assoc.set_link("p1", False)
            assoc.tick(2)
            assoc.set_link("p1", True)
            assoc.tick(2)
        self.assertEqual(assoc.current_path, "p2")
        self.assertEqual(assoc.paths["p1"].status, PathStatus.ACTIVE)
        self.assertEqual(assoc.state, AssocState.ESTABLISHED)


class HeartbeatTimeoutTest(unittest.TestCase):
    """心跳超时：无数据时仅靠心跳错误计数把路径置为 INACTIVE。"""

    def test_heartbeat_timeout_marks_path_inactive(self):
        assoc = make_assoc()
        assoc.set_link("p1", False)
        assoc.tick(13)
        p1 = assoc.paths["p1"]
        self.assertGreaterEqual(p1.hb_sent, 3)
        self.assertEqual(p1.status, PathStatus.INACTIVE)
        self.assertEqual(assoc.current_path, "p2")
        self.assertEqual(assoc.unacked_tsns, [])
        self.assertEqual(assoc.state, AssocState.ESTABLISHED)

    def test_heartbeat_success_does_not_confirm_data(self):
        assoc = make_assoc()
        assoc.set_link("p1", False)
        assoc.send_message(msg_id=1, size=100)
        assoc.tick(2)  # RTO 超时一次，数据丢失未确认
        assoc.set_link("p1", True)
        assoc.tick(3)  # t=5：心跳成功，错误计数清零
        self.assertEqual(assoc.paths["p1"].error_count, 0)
        self.assertGreaterEqual(assoc.paths["p1"].hb_acked, 1)
        # 心跳成功不代表数据路径可用：数据仍未确认
        self.assertEqual(assoc.unacked_tsns, [1])
        self.assertEqual(assoc.current_path, "p1")
        assoc.tick(3)  # 链路已恢复，RTO 重传成功
        self.assertEqual(assoc.unacked_tsns, [])


class SackLossTest(unittest.TestCase):
    """SACK 丢失：数据到达对端但确认丢失，RTO 重传，对端报告重复 TSN。"""

    def test_sack_loss_triggers_retransmission_and_dup_report(self):
        assoc = make_assoc()
        assoc.drop_sacks = True
        assoc.send_message(msg_id=1, size=100)
        assoc.tick(8)
        self.assertGreaterEqual(len(retransmissions(assoc, tsn=1)), 2)
        self.assertEqual(assoc.unacked_tsns, [1])
        assoc.drop_sacks = False
        assoc.tick(12)
        self.assertEqual(assoc.unacked_tsns, [])
        # 心跳一直成功，路径错误计数被清零，不发生切换
        self.assertEqual(assoc.current_path, "p1")
        self.assertIn(1, assoc.dup_tsns)
        self.assertEqual(assoc.state, AssocState.ESTABLISHED)


class LongRetransmissionTest(unittest.TestCase):
    """长时间重传：连续 RTO 指数退避并封顶，关联最终因超限关闭。"""

    def test_consecutive_rto_backoff_and_eventual_close(self):
        assoc = Association(rto_initial=2, rto_max=8, assoc_max_retrans=30,
                            path_max_retrans=30, hb_interval=4)
        assoc.add_path("only", "10.0.0.1 <-> 192.0.2.1", primary=True)
        assoc.set_link("only", False)
        assoc.send_message(msg_id=1, size=100)
        assoc.tick(30)
        rto_events = [e["rto"] for e in assoc.events
                      if e["event"] == "rto-expired"]
        self.assertEqual(rto_events[:4], [2, 4, 8, 8])
        self.assertEqual(assoc.rto, 8)
        self.assertGreaterEqual(len(retransmissions(assoc, tsn=1)), 4)
        self.assertEqual(assoc.unacked_tsns, [1])
        self.assertEqual(assoc.state, AssocState.ESTABLISHED)
        assoc.tick(60)
        self.assertEqual(assoc.state, AssocState.CLOSED)


class SackEdgeCasesTest(unittest.TestCase):
    """乱序 SACK、重复 TSN 报告与部分消息重传。"""

    def test_out_of_order_sack_cum_tsn_never_regresses(self):
        assoc = make_assoc()
        assoc.send_message(msg_id=1, size=100, fragments=4)
        assoc.inject_sack(cum_tsn=2)
        self.assertEqual(assoc.unacked_tsns, [3, 4])
        assoc.inject_sack(cum_tsn=1)  # 乱序到达的过期 SACK：累积点不后退
        self.assertEqual(assoc.cum_tsn_acked, 2)
        self.assertEqual(assoc.unacked_tsns, [3, 4])
        self.assertTrue(any(e["event"] == "sack-stale-out-of-order"
                            for e in assoc.events))
        assoc.inject_sack(cum_tsn=2, gaps=[4])  # 间隙块确认 TSN 4
        self.assertEqual(assoc.unacked_tsns, [3])

    def test_duplicate_tsn_report_is_recorded(self):
        assoc = make_assoc()
        assoc.send_message(msg_id=1, size=100)
        assoc.inject_sack(cum_tsn=0, dup_tsns=[1])
        self.assertIn(1, assoc.dup_tsns)
        self.assertEqual(assoc.unacked_tsns, [1])

    def test_partial_message_retransmission(self):
        assoc = make_assoc()
        assoc.drop_sacks = True  # 屏蔽自动 SACK，手动控制确认
        assoc.send_message(msg_id=1, size=100, fragments=4)
        assoc.inject_sack(cum_tsn=2, gaps=[4])  # 只留分片 3 未确认
        self.assertEqual(assoc.unacked_tsns, [3])
        assoc.tick(2)  # RTO 超时
        self.assertEqual(len(retransmissions(assoc, tsn=3)), 1)
        for tsn in (1, 2, 4):
            self.assertEqual(retransmissions(assoc, tsn), [],
                             "已确认的分片不应被重传")
        assoc.inject_sack(cum_tsn=4)
        self.assertEqual(assoc.unacked_tsns, [])


class ReportTest(unittest.TestCase):
    """输出报告包含路径状态、实际发送路径、未确认块与关联最终状态。"""

    def test_report_contents(self):
        assoc = make_assoc()
        assoc.set_link("p1", False)
        assoc.send_message(msg_id=1, size=100)
        assoc.tick(10)
        report = assoc.report()
        self.assertEqual(report["association_state"], "ESTABLISHED")
        self.assertEqual(report["current_path"], "p2")
        self.assertEqual(report["paths"]["p1"]["status"], "INACTIVE")
        self.assertEqual(report["paths"]["p2"]["status"], "ACTIVE")
        self.assertEqual(report["unacked_tsns"], [])
        self.assertTrue(any(t["path"] == "p2" and t["retrans"]
                            for t in report["transmissions"]))


if __name__ == "__main__":
    unittest.main()
