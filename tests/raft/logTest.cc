//
// 分片日志 + 状态元信息 测试（对应 src/raft/log.{hpp,cc} 的当前实现）
//
//   覆盖：初始化/空日志约定、追加与落盘（幂等/越界）、查询（term/区间/匹配）、
//         重启恢复、崩溃尾部修复、冲突截断（含"已提交不可截断"）、
//         按字节切段 + 内存窗口（只有最后一段常驻、旧段走磁盘）、coLoadLogSeg/coFreeLogSeg、
//         StateMeta 三态加载 / 原子落盘 / 损坏拒绝
//
//   注意：本文件的 co_await 全都落在"同步就能完成"的 Task 上（NodeLog 内部是同步系统调用，
//   不会真正挂起），所以 runAll() 这个 eager Task 会一路跑到 co_return，不需要 IOManager。
//   以后若把日志 I/O 改成异步，这里要换成 create_coro + IOManager 调度。
//
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>

#include "raft/log.hpp"

using namespace craft;

static int g_pass = 0;
static int g_fail = 0;
static std::string g_dir;

static void check(bool ok, const std::string& name, const std::string& detail = "") {
    if (ok) {
        ++g_pass;
        std::printf("  \xe2\x9c\x93 %s\n", name.c_str());
    } else {
        ++g_fail;
        std::printf("  \xe2\x9c\x97 %s   %s\n", name.c_str(), detail.c_str());
    }
}

static std::string S(uint64_t v) { return std::to_string(v); }

static uint64_t fileSize(const std::string& path) {
    struct stat st{};
    return (::stat(path.c_str(), &st) == 0) ? static_cast<uint64_t>(st.st_size) : 0;
}

static LogEntry mkEntry(uint64_t term, const std::string& command) {
    LogEntry e;
    e.set_term(term);
    e.set_command(command);
    return e;
}


static m_sylar::Task<void> runAll() {
    // ============================== A. 初始化 / 空日志约定 ==============================
    std::printf("A. 初始化与空日志约定\n");
    NodeLog log;
    check(co_await log.init(7) == m_sylar::IOState::SUCCESS, "A1. init(7) 空目录成功");
    check(log.getLastLogIdx() == 0 && log.getFirstIdx() == 1 && log.getLastLogTerm() == 0,
          "A2. 空日志约定：last=0, first=1, last_term=0",
          "last=" + S(log.getLastLogIdx()) + " first=" + S(log.getFirstIdx()));
    check(log.getPersistIndex() == 0 && log.getCommitIndex() == 0, "A3. 位点初始为 0");
    check(log.matchLog(0, 0), "A4. matchLog(0,0) == true（哨兵）");

    // ============================== B. 追加 / 落盘 ==============================
    std::printf("B. 追加与落盘\n");
    for (int i = 1; i <= 5; ++i) {
        log.appendLog(mkEntry(static_cast<uint64_t>(i), "cmd" + std::to_string(i)));
    }
    check(log.getLastLogIdx() == 5 && log.getLastLogTerm() == 5, "B1. appendLog 5 条后 last=5, term=5");
    check(log.getPersistIndex() == 0, "B2. 落盘前 persistIdx 仍为 0");
    check(log.persistLog(5) == m_sylar::IOState::SUCCESS, "B3. persistLog(5) 成功");
    check(log.getPersistIndex() == 5, "B4. persistIdx 推进到 5", "persist=" + S(log.getPersistIndex()));
    check(!log.getEntriesFrom(1, 5).empty(), "B5. 落盘后能读回内容");
    check(log.persistLog(5) == m_sylar::IOState::SUCCESS, "B6. persistLog 重复调用幂等");
    check(log.persistLog(6) == m_sylar::IOState::FAILED, "B7. persistLog(6) 超过 last -> FAILED");
    check(log.persistLog(2) == m_sylar::IOState::SUCCESS, "B8. persistLog(2)（已落盘区间）-> 幂等 SUCCESS");

    // ============================== C. 查询 ==============================
    std::printf("C. 查询（term / 区间 / 匹配）\n");
    auto t3 = log.getTermAt(3);
    check(t3.has_value() && *t3 == 3, "C1. getTermAt(3) == 3", t3 ? S(*t3) : "nullopt");
    {
        auto t0 = log.getTermAt(0);
        check(t0.has_value() && *t0 == 0, "C2. getTermAt(0) == 0（哨兵）");
    }
    check(!log.getTermAt(9).has_value(), "C3. getTermAt(9) == nullopt（越界）");
    check(log.matchLog(5, 5) && !log.matchLog(5, 4), "C4. matchLog 命中 / 不命中");
    check(!log.matchLog(9, 1), "C5. matchLog(9,1) == false（本地没这么长）");
    {
        auto es = log.getEntriesFrom(2, 3);
        check(es.size() == 3 && es.front().command() == "cmd2" && es.back().command() == "cmd4",
              "C6. getEntriesFrom(2,3) 取到 cmd2..cmd4", "n=" + S(es.size()));
    }
    check(log.getEntriesFrom(9, 3).empty(), "C7. getEntriesFrom 越界 -> 空");
    check(log.getEntriesFrom(1, 100).size() == 5, "C8. count 超过剩余条数时自动截到 last");

    // ============================== D. 提交位点 ==============================
    std::printf("D. 提交位点（仅内存）\n");
    check(log.commitLog(3), "D1. commitLog(3) == true");
    check(log.getCommitIndex() == 3, "D2. commitIndex == 3");
    check(!log.commitLog(9), "D3. commitLog(9)（无此日志）== false");
    check(log.commitLog(3), "D4. commitLog 重复调用 == true（已提交）");

    // ============================== E. 重启恢复 ==============================
    std::printf("E. 重启恢复\n");
    NodeLog log2;
    check(co_await log2.init(7) == m_sylar::IOState::SUCCESS, "E1. 重启 init 成功");
    check(log2.getLastLogIdx() == 5 && log2.getLastLogTerm() == 5, "E2. 恢复 last=5, last_term=5",
          "last=" + S(log2.getLastLogIdx()) + " term=" + S(log2.getLastLogTerm()));
    check(log2.getPersistIndex() == 5, "E3. 恢复后 persistIdx == last（磁盘上的都算已落盘）");
    check(log2.getCommitIndex() == 0, "E4. commitIndex 易失：重启归 0");
    {
        auto es = log2.getEntriesFrom(1, 5);
        check(es.size() == 5 && es[0].command() == "cmd1" && es[4].term() == 5,
              "E5. 恢复后内容 / term 一致");
    }

    // ============================== F. 冲突截断 ==============================
    std::printf("F. 冲突截断（未提交部分）\n");
    check(log2.truncateFrom(3) == m_sylar::IOState::SUCCESS, "F1. truncateFrom(3) 成功");
    check(log2.getLastLogIdx() == 2 && log2.getLastLogTerm() == 2, "F2. 截断后 last=2, last_term=2",
          "last=" + S(log2.getLastLogIdx()) + " term=" + S(log2.getLastLogTerm()));
    check(log2.getPersistIndex() <= log2.getLastLogIdx(), "F3. persistIdx 不会超过 last");
    {
        NodeLog again;
        co_await again.init(7);
        check(again.getLastLogIdx() == 2, "F4. 截断已落盘：重启后 last 仍为 2",
              "last=" + S(again.getLastLogIdx()));
    }
    check(log2.truncateFrom(99) == m_sylar::IOState::SUCCESS, "F5. truncateFrom 超过 last -> 幂等 SUCCESS");

    // 已提交的不能截断
    {
        NodeLog c;
        co_await c.init(7);
        c.commitLog(2);
        check(c.truncateFrom(2) == m_sylar::IOState::FAILED, "F6. 截断已提交位置 -> FAILED");
        check(c.truncateFrom(1) == m_sylar::IOState::FAILED, "F7. 截断点 <= commitIdx -> FAILED");
        check(c.getLastLogIdx() == 2, "F8. 失败的截断没有副作用");
    }

    // ============================== G. 切段 + 内存窗口 ==============================
    std::printf("G. 按字节切段 + 内存窗口（只有最后一段常驻，旧段走磁盘）\n");
    ::unlink((g_dir + "/7_1.seg").c_str());     // 清掉前面用例留下的段，从空日志开始
    NodeLog big;
    co_await big.init(7);
    const std::string payload(1024 * 1024, 'x');        // 1MB/条：10 条必然超过 8MB 阈值
    for (int i = 1; i <= 10; ++i) {
        big.appendLog(mkEntry(9, payload));
    }
    check(big.persistLog(10) == m_sylar::IOState::SUCCESS, "G1. 10 x 1MB 落盘成功（触发切段）");
    {
        NodeLog r;
        co_await r.init(7);
        check(r.getLastLogIdx() == 10, "G2. 重开 last=10", "last=" + S(r.getLastLogIdx()));
        check(r.getFirstIdx() > 1, "G3. 内存窗口只含最后一段（first > 1）",
              "first=" + S(r.getFirstIdx()));
        auto old = r.getEntriesFrom(1, 2);
        check(old.size() == 2 && old.front().command().size() == payload.size(),
              "G4. 旧段数据从磁盘读回", "n=" + S(old.size()));
        check(r.matchLog(1, 9), "G5. matchLog(1,9) 走磁盘也命中");
        check(co_await r.coLoadLogSeg(1) == m_sylar::IOState::SUCCESS, "G6. coLoadLogSeg(1) 前扩成功");
        check(r.getFirstIdx() == 1, "G7. 前扩后 first == 1", "first=" + S(r.getFirstIdx()));
        check(r.getEntriesFrom(1, 2).size() == 2, "G8. 前扩后再取就走内存");
        check(co_await r.coFreeLogSeg(8) == true, "G9. coFreeLogSeg(8) 释放前缀");
        check(r.getFirstIdx() == 9, "G10. 释放后 first == 9", "first=" + S(r.getFirstIdx()));
        check(co_await r.coFreeLogSeg(10) == false, "G11. 不允许把最后一条也释放掉");
    }

    // ============================== H. 崩溃尾部修复 ==============================
    std::printf("H. 崩溃尾部修复（半截记录）\n");
    const std::string seg = g_dir + "/11_1.seg";
    ::unlink(seg.c_str());
    {
        NodeLog w;
        co_await w.init(11);
        for (int i = 1; i <= 3; ++i) {
            w.appendLog(mkEntry(static_cast<uint64_t>(i), std::string(40, static_cast<char>('a' + i))));
        }
        check(w.persistLog(3) == m_sylar::IOState::SUCCESS, "H1. 写 3 条");
    }
    const uint64_t full = fileSize(seg);
    check(full > kLogHeaderSize, "H2. 段文件有内容", "size=" + S(full));
    check(::truncate(seg.c_str(), static_cast<off_t>(full - 3)) == 0, "H3. 人为把最后一条砍掉 3 字节");
    {
        NodeLog r;
        check(co_await r.init(11) == m_sylar::IOState::SUCCESS, "H4. 半截记录不会导致启动失败（自动修复）");
        check(r.getLastLogIdx() == 2 && r.getLastLogTerm() == 2, "H5. 修复后 last=2, last_term=2",
              "last=" + S(r.getLastLogIdx()));
        check(r.getEntriesFrom(1, 5).size() == 2, "H6. 前两条仍可读");
        r.appendLog(mkEntry(4, "after-repair"));
        check(r.persistLog(3) == m_sylar::IOState::SUCCESS, "H7. 修复后能继续追加");
    }
    {
        NodeLog r;
        co_await r.init(11);
        check(r.getLastLogIdx() == 3 && r.getLastLogTerm() == 4, "H8. 再重启 last=3, last_term=4",
              "last=" + S(r.getLastLogIdx()) + " term=" + S(r.getLastLogTerm()));
    }

    // ============================== I. 状态元信息 ==============================
    std::printf("I. StateMeta（term/voted_for 单独存、原子写、损坏拒绝）\n");
    StateMeta sm;
    check(co_await sm.load(7) == StateMeta::State::Create, "I1. 无状态文件 -> Create");
    check(sm.getCurrentTerm() == 0 && sm.getVotedFor() == -1,
          "I2. 全新节点默认 term=0, voted_for=-1（-1 表示未投票）",
          "term=" + S(sm.getCurrentTerm()) + " voted=" + std::to_string(sm.getVotedFor()));
    sm.setCurrentTerm(42);
    sm.setVotedFor(3);
    check(co_await sm.save() == m_sylar::IOState::SUCCESS, "I3. save 成功");
    {
        StateMeta r;
        check(co_await r.load(7) == StateMeta::State::Success, "I4. 重新 load -> Success");
        check(r.getCurrentTerm() == 42 && r.getVotedFor() == 3, "I5. term/voted_for 正确恢复",
              "term=" + S(r.getCurrentTerm()) + " voted=" + std::to_string(r.getVotedFor()));
    }
    {   // 破坏文件 -> Failed（调用方必须拒绝启动，不能当作全新节点）
        const std::string p = g_dir + "/7_state_meta.ras";
        const int fd = ::open(p.c_str(), O_RDWR);
        if (fd >= 0) {
            const char c = 'Z';
            ::pwrite(fd, &c, 1, 0);
            ::close(fd);
        }
        StateMeta bad;
        check(co_await bad.load(7) == StateMeta::State::Failed, "I6. 文件损坏 -> Failed");
    }

    co_return;
}


int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);

    g_dir = "/tmp/kvraft_log_test_" + std::to_string(::getpid());
    const std::string prep = "rm -rf " + g_dir + " && mkdir -p " + g_dir;
    if (::system(prep.c_str()) != 0 || ::chdir(g_dir.c_str()) != 0) {
        std::printf("准备临时目录失败: %s\n", g_dir.c_str());
        return 1;
    }
    std::printf("临时目录: %s\n", g_dir.c_str());

    runAll();       // eager Task：体里全是同步完成的 co_await

    const std::string clean = "rm -rf " + g_dir;
    if (::system(clean.c_str()) != 0) { /* 清理失败无关紧要 */ }

    std::printf("\n结果: pass=%d fail=%d\n", g_pass, g_fail);
    std::_Exit(g_fail == 0 ? 0 : 1);     // 和别的测试一致：避开 m_sylar 静态对象析构
}
