# -*- coding: utf-8 -*-
"""演示：主路径断开后切换到备用路径并完成重传。

运行：python -m sctp_sim
"""

import json

from .association import Association


def main():
    assoc = Association(rto_initial=2, rto_max=8, assoc_max_retrans=8,
                        path_max_retrans=2, hb_interval=4)
    assoc.add_path("primary", "10.0.0.1 <-> 192.0.2.1", primary=True)
    assoc.add_path("backup", "10.0.0.2 <-> 192.0.2.2")

    print("== t=0 发送一条 4 分片消息，随后主路径链路断开 ==")
    assoc.send_message(msg_id=1, size=256, fragments=4)
    assoc.set_link("primary", False)

    for _ in range(10):
        assoc.tick()
        print("t=%2d state=%s current=%s unacked=%s" % (
            assoc.time, assoc.state.value, assoc.current_path,
            assoc.unacked_tsns))

    print("\n== 主路径恢复（不会立刻切回，当前路径仍有效）==")
    assoc.set_link("primary", True)
    for _ in range(6):
        assoc.tick()
        print("t=%2d state=%s current=%s unacked=%s" % (
            assoc.time, assoc.state.value, assoc.current_path,
            assoc.unacked_tsns))

    print("\n== 最终报告 ==")
    report = assoc.report()
    report.pop("events")
    print(json.dumps(report, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
