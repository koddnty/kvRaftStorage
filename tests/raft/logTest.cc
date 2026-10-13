//
// 分片日志文件头（元信息）测试
// changed: 本文件为新增的测试（分片文件头 + NodeLog::persistLog/commitLog）
//
//   覆盖：create 写头 / readHeader 读回 base_idx+entry_count / persist 自动计数 /
//         readLogFile 跳过文件头 / 重开文件仍然一致 / 坏 fd / 空文件 / 截断文件 /
//         版本不认识 / 老格式（裸记录无头）兼容 / create 的"重建"语义
//
//   注意：本文件的 co_await 全都落在"同步就能完成"的 Task 上（纯磁盘 IO，没有真异步），
//   所以 runAll() 这个 eager Task 会在 main 里一路跑到 co_return，不需要 IOManager。
//   以后要是在这里加真异步操作，得改成 create_coro + IOManager 调度。
//
#include <arpa/inet.h>
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
static std::string g_path;

static void check(bool ok, const std::string& name, const std::string& detail = "") {
    if (ok) {
        ++g_pass;
        std::printf("  \xe2\x9c\x93 %s\n", name.c_str());
    } else {
        ++g_fail;
        std::printf("  \xe2\x9c\x97 %s   %s\n", name.c_str(), detail.c_str());
    }
}

static std::string u64s(uint64_t v) { return std::to_string(v); }

static int openNew() {
    return ::open(g_path.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
}

static int openExist() {
    return ::open(g_path.c_str(), O_RDWR);
}

static uint64_t sizeOf(int fd) {
    struct stat st{};
    if (::fstat(fd, &st) < 0) {
        return 0;
    }
    return static_cast<uint64_t>(st.st_size);
}

static uint64_t readU64At(int fd, off_t off) {
    char buf[8] = {};
    if (::pread(fd, buf, sizeof(buf), off) != static_cast<ssize_t>(sizeof(buf))) {
        return 0;
    }
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) {
        v = (v << 8) | static_cast<uint8_t>(buf[i]);
    }
    return v;
}

// 造一条"老格式"裸记录：没有任何文件头，就是 [uint32 大端长度][protobuf]
static void appendRawRecord(int fd, uint64_t term, const std::string& command) {
    LogEntry e;
    e.set_term(term);
    e.set_command(command);
    const std::string data = e.SerializeAsString();
    const uint32_t len = htonl(static_cast<uint32_t>(data.size()));
    const ssize_t w1 = ::write(fd, &len, sizeof(len));
    const ssize_t w2 = ::write(fd, data.data(), data.size());
    (void)w1;
    (void)w2;
}

static uint64_t recordBytes(const std::string& command, uint64_t term) {
    LogEntry e;
    e.set_term(term);
    e.set_command(command);
    return sizeof(uint32_t) + e.SerializeAsString().size();
}


static m_sylar::Task<void> runAll() {
    // ============================== A. create 写文件头 ==============================
    std::printf("A. create：写文件头元信息\n");
    {
        const int fd = openNew();
        check(fd >= 0, "A0. open 成功", g_path);
        if (fd < 0) {
            co_return;
        }

        const bool ok = co_await LogEntryWrapper::create(fd, /*base_idx=*/100);
        check(ok, "A1. create(fd, base_idx=100) 返回 true");
        check(sizeOf(fd) == kLogHeaderSize, "A2. 文件大小 == 32（只有头，没有记录）",
              "size=" + u64s(sizeOf(fd)));

        char magic[4] = {};
        const ssize_t got = ::pread(fd, magic, sizeof(magic), 0);
        check(got == 4 && magic[0] == 'K' && magic[1] == 'V' && magic[2] == 'L' && magic[3] == 'G',
              "A3. 磁盘上偏移 0 就是 magic 'KVLG'");

        check(readU64At(fd, 8) == 100, "A4. 磁盘上偏移 8 就是 base_idx（大端）",
              "base_idx=" + u64s(readU64At(fd, 8)));
        check(readU64At(fd, 16) == 0, "A5. 磁盘上偏移 16 的 entry_count 从 0 开始",
              "count=" + u64s(readU64At(fd, 16)));

        LogFileMeta meta{};
        const bool hok = co_await LogEntryWrapper::readHeader(fd, meta);
        check(hok && meta.base_idx == 100 && meta.entry_count == 0 &&
                  meta.header_size == kLogHeaderSize && meta.empty() && meta.lastIdx() == 99,
              "A6. readHeader 读回 base_idx=100 / entry_count=0 / lastIdx=99",
              "ok=" + u64s(hok) + " base=" + u64s(meta.base_idx) +
                  " count=" + u64s(meta.entry_count) + " last=" + u64s(meta.lastIdx()));

        ::close(fd);
    }

    // ============================== B. persist 自动维护 entry_count ==============================
    std::printf("B. persist：追加日志并自动把 entry_count +1\n");
    {
        const int fd = openExist();
        check(fd >= 0, "B0. 重新 open 成功");
        if (fd < 0) {
            co_return;
        }

        const std::string cmds[3] = {"put a=1", "put b=2", "put c=3"};
        bool all_ok = true;
        uint64_t expect_size = kLogHeaderSize;
        for (int i = 0; i < 3; ++i) {
            LogEntryWrapper w(static_cast<uint64_t>(i + 1), cmds[i], fd);
            if (!co_await w.persist()) {
                all_ok = false;
            }
            expect_size += recordBytes(cmds[i], static_cast<uint64_t>(i + 1));
        }
        check(all_ok, "B1. persist 3 条都返回 true");
        check(sizeOf(fd) == expect_size, "B2. 文件大小 = 32 + 3 条记录",
              "size=" + u64s(sizeOf(fd)) + " expect=" + u64s(expect_size));

        LogFileMeta meta{};
        const bool hok = co_await LogEntryWrapper::readHeader(fd, meta);
        check(hok && meta.base_idx == 100 && meta.entry_count == 3,
              "B3. ★ readHeader: base_idx 不变(100)、entry_count 自动变成 3",
              "base=" + u64s(meta.base_idx) + " count=" + u64s(meta.entry_count));
        check(readU64At(fd, 16) == 3, "B4. ★ 磁盘上偏移 16 的 entry_count 也真的是 3（就地更新生效）",
              "count=" + u64s(readU64At(fd, 16)));
        check(meta.lastIdx() == 102, "B5. 本片覆盖索引 [100, 102]",
              "lastIdx=" + u64s(meta.lastIdx()));

        std::vector<LogEntry> entries = co_await LogEntryWrapper::readLogFile(fd);
        bool same = entries.size() == 3;
        if (same) {
            for (int i = 0; i < 3; ++i) {
                if (entries[i].term() != static_cast<uint64_t>(i + 1) || entries[i].command() != cmds[i]) {
                    same = false;
                }
            }
        }
        check(same, "B6. ★ readLogFile 跳过文件头，按序读出 3 条（term/command 对得上）",
              "n=" + u64s(entries.size()));

        // 索引换算：LogEntry 里没有 index 字段，索引 = base_idx + 位置
        check(entries.size() == meta.entry_count &&
                  meta.base_idx + entries.size() - 1 == meta.lastIdx(),
              "B7. 索引换算自洽：base_idx + 位置 = [100,102]");

        ::close(fd);
    }

    // ============================== C. 重开文件（持久化后元信息仍在） ==============================
    std::printf("C. 重开：元信息与内容都还在\n");
    {
        const int fd = openExist();
        LogFileMeta meta{};
        const bool hok = co_await LogEntryWrapper::readHeader(fd, meta);
        std::vector<LogEntry> entries = co_await LogEntryWrapper::readLogFile(fd);
        check(hok && meta.base_idx == 100 && meta.entry_count == 3,
              "C1. 新 fd 上 readHeader 仍是 base_idx=100 / count=3",
              "base=" + u64s(meta.base_idx) + " count=" + u64s(meta.entry_count));
        check(entries.size() == 3 && entries[2].command() == "put c=3",
              "C2. 新 fd 上 readLogFile 仍是 3 条、顺序不变",
              "n=" + u64s(entries.size()));
        ::close(fd);
    }

    // ============================== D. updateEntryCount 单独用 ==============================
    std::printf("D. updateEntryCount：单独就地改计数\n");
    {
        const int fd = openExist();
        check(co_await LogEntryWrapper::updateEntryCount(fd, 7), "D1. updateEntryCount(fd,7) 返回 true");
        LogFileMeta meta{};
        const bool hok = co_await LogEntryWrapper::readHeader(fd, meta);
        check(hok && meta.entry_count == 7 && meta.base_idx == 100,
              "D2. 只改了 entry_count，base_idx 不动",
              "base=" + u64s(meta.base_idx) + " count=" + u64s(meta.entry_count));
        check(sizeOf(fd) == sizeOf(fd), "D3. 就地改写：文件大小不变（没有追加/截断）");
        check(co_await LogEntryWrapper::updateEntryCount(fd, 3), "D4. 改回 3");
        ::close(fd);
    }

    // ============================== E. 坏 fd / 空文件 / 截断 / 版本 / 老格式 ==============================
    std::printf("E. 异常输入：都要安全返回 false，不能崩\n");
    {
        LogFileMeta meta{};
        check(!co_await LogEntryWrapper::create(-1, 1), "E1. create(-1) == false");
        check(!co_await LogEntryWrapper::readHeader(-1, meta), "E2. readHeader(-1) == false");
        check(!co_await LogEntryWrapper::updateEntryCount(-1, 1), "E3. updateEntryCount(-1) == false");
    }

    // 空文件（0 字节）
    const std::string empty_path = g_path + ".empty";
    {
        ::unlink(empty_path.c_str());
        const int fd = ::open(empty_path.c_str(), O_CREAT | O_RDWR, 0644);
        LogFileMeta meta{};
        check(!co_await LogEntryWrapper::readHeader(fd, meta),
              "E4. 空文件 readHeader == false（当作不是分片文件）");
        std::vector<LogEntry> entries = co_await LogEntryWrapper::readLogFile(fd);
        check(entries.empty(), "E5. 空文件 readLogFile 返回 0 条（不抛）",
              "n=" + u64s(entries.size()));
        ::close(fd);
        ::unlink(empty_path.c_str());
    }

    // 截断的文件头（10 字节）
    const std::string trunc_path = g_path + ".trunc";
    {
        ::unlink(trunc_path.c_str());
        const int fd = ::open(trunc_path.c_str(), O_CREAT | O_RDWR, 0644);
        const ssize_t w = ::write(fd, "KVLG12345", 10);
        (void)w;
        LogFileMeta meta{};
        check(!co_await LogEntryWrapper::readHeader(fd, meta), "E6. 只有 10 字节的残头 == false");
        ::close(fd);
        ::unlink(trunc_path.c_str());
    }

    // 版本不认识
    const std::string ver_path = g_path + ".ver";
    {
        ::unlink(ver_path.c_str());
        const int fd = ::open(ver_path.c_str(), O_CREAT | O_RDWR, 0644);
        char hdr[kLogHeaderSize] = {};
        hdr[0] = 'K'; hdr[1] = 'V'; hdr[2] = 'L'; hdr[3] = 'G';
        hdr[5] = 99;                            // version = 99（大端，高字节 0）
        hdr[7] = static_cast<char>(kLogHeaderSize);
        const ssize_t w = ::write(fd, hdr, sizeof(hdr));
        (void)w;
        LogFileMeta meta{};
        check(!co_await LogEntryWrapper::readHeader(fd, meta),
              "E7. 未来版本的 version=99 == false（不敢按当前布局解）");
        ::close(fd);
        ::unlink(ver_path.c_str());
    }

    // 老格式：裸记录，没有文件头 —— 必须还能读
    const std::string legacy_path = g_path + ".legacy";
    {
        ::unlink(legacy_path.c_str());
        const int fd = ::open(legacy_path.c_str(), O_CREAT | O_RDWR, 0644);
        appendRawRecord(fd, 7, "legacy-1");
        appendRawRecord(fd, 8, "legacy-2");

        LogFileMeta meta{};
        check(!co_await LogEntryWrapper::readHeader(fd, meta),
              "E8. 无头的老文件 readHeader == false（调用方应退回偏移 0）");

        std::vector<LogEntry> entries = co_await LogEntryWrapper::readLogFile(fd);
        check(entries.size() == 2 && entries[0].command() == "legacy-1" &&
                  entries[1].term() == 8,
              "E9. ★ 老格式仍能读：readLogFile 从偏移 0 解析出 2 条",
              "n=" + u64s(entries.size()));
        ::close(fd);
        ::unlink(legacy_path.c_str());
    }

    // ============================== F. create 的"重建"语义 ==============================
    std::printf("F. create 重建：老内容（含旧头）全部丢掉\n");
    {
        const int fd = openExist();
        check(co_await LogEntryWrapper::create(fd, /*base_idx=*/500), "F1. 对已有文件再 create 成功");
        LogFileMeta meta{};
        const bool hok = co_await LogEntryWrapper::readHeader(fd, meta);
        std::vector<LogEntry> entries = co_await LogEntryWrapper::readLogFile(fd);
        check(sizeOf(fd) == kLogHeaderSize && hok && meta.base_idx == 500 &&
                  meta.entry_count == 0 && entries.empty(),
              "F2. ★ 重建后：size=32、base_idx=500、entry_count=0、读不到旧记录",
              "size=" + u64s(sizeOf(fd)) + " base=" + u64s(meta.base_idx) +
                  " count=" + u64s(meta.entry_count) + " n=" + u64s(entries.size()));
        ::close(fd);
    }

    // ============================== G. NodeLog::persistLog / commitLog ==============================
    std::printf("G. NodeLog：persistLog（同步落盘、不跳号）/ commitLog（不许提交未落盘的）\n");
    {
        // G1. 没 attach 分片文件时不能落盘
        {
            NodeLog bad;
            const int tfd = openNew();
            (void)co_await LogEntryWrapper::create(tfd, 1);
            bad.appendLog(LogEntryWrapper(1, "orphan", tfd));
            check(bad.persistLog(1) == m_sylar::IOState::FAILED,
                  "G1. 没 attach 分片 fd：persistLog(1) == FAILED（不是崩溃也不是假装成功）");
            ::close(tfd);
        }

        const std::string shard = g_path + ".shard";
        ::unlink(shard.c_str());
        const int fd = ::open(shard.c_str(), O_CREAT | O_RDWR, 0644);
        check(fd >= 0 && co_await LogEntryWrapper::create(fd, /*base_idx=*/1),
              "G2. 建一个 base_idx=1 的空分片");

        NodeLog log;
        log.attach(fd, /*base_idx=*/1);
        check(log.persistLog(0) == m_sylar::IOState::SUCCESS,
              "G3. persistLog(0) == SUCCESS（没有日志要写，幂等）");

        const std::string cmds[5] = {"e1", "e2", "e3", "e4", "e5"};
        for (int i = 0; i < 3; ++i) {
            log.appendLog(LogEntryWrapper(static_cast<uint64_t>(i + 1), cmds[i], fd));
        }
        check(log.getLastLogIdx() == 3, "G4. appendLog 3 条后 getLastLogIdx() == 3",
              "last=" + u64s(log.getLastLogIdx()));

        // G5. 只落盘到 2：不允许跳号，也不许越过
        check(log.persistLog(2) == m_sylar::IOState::SUCCESS, "G5. persistLog(2) == SUCCESS");
        LogFileMeta meta{};
        co_await LogEntryWrapper::readHeader(fd, meta);
        check(meta.entry_count == 2 && log.getPersistIndex() == 2,
              "G6. ★ 文件头 entry_count == 2（真的只写了前 2 条）",
              "count=" + u64s(meta.entry_count) + " persist_idx=" + u64s(log.getPersistIndex()));

        // G7. 还没 append 的索引落不了盘
        check(log.persistLog(4) == m_sylar::IOState::FAILED,
              "G7. persistLog(4)（只 append 了 3 条）== FAILED");
        co_await LogEntryWrapper::readHeader(fd, meta);
        check(meta.entry_count == 2, "G8. 失败后文件没有被写脏，entry_count 仍是 2",
              "count=" + u64s(meta.entry_count));

        // G9. 补到 3
        check(log.persistLog(3) == m_sylar::IOState::SUCCESS && log.getPersistIndex() == 3,
              "G9. persistLog(3) 补齐第 3 条");

        // G10. 幂等
        check(log.persistLog(3) == m_sylar::IOState::SUCCESS, "G10. persistLog(3) 重复调用仍 SUCCESS（幂等）");
        co_await LogEntryWrapper::readHeader(fd, meta);
        check(meta.entry_count == 3, "G11. 重复落盘不会把条目重复写进去（entry_count 仍是 3）",
              "count=" + u64s(meta.entry_count));

        // G12. commitLog：越过已落盘位点的提交必须被拒
        check(log.commitLog(2) && log.getCommitIndex() == 2, "G12. commitLog(2) == true，commitIndex=2");
        check(!log.commitLog(4), "G13. ★ commitLog(4)：没有这条日志 == false");
        check(!log.commitLog(4), "G14. 再调一次 false（因为没有这条日志，而不是因为已提交）");
        check(log.commitLog(3) && log.getCommitIndex() == 3, "G15. commitLog(3)（已落盘）== true");
        check(log.commitLog(3), "G16. commitLog(3) 重复调用 == true（已经提交）");
        check(log.commitLog(1), "G17. commitLog(1)（比当前提交位点小）== true（已经提交）");
        check(log.getCommitIndex() == 3, "G18. ★ 提交位点不会被小的 idx 回退",
              "commit=" + u64s(log.getCommitIndex()));

        // G19. 未落盘的日志不许提交
        log.appendLog(LogEntryWrapper(4, cmds[3], fd));
        check(!log.commitLog(4), "G19. ★ 第 4 条还没落盘：commitLog(4) == false");
        check(log.persistLog(4) == m_sylar::IOState::SUCCESS && log.commitLog(4) &&
                  log.getCommitIndex() == 4,
              "G20. 落盘之后 commitLog(4) == true");

        // G21. 从 RPC 收到的 entry：用 LogEntryWrapper(const LogEntry&) 构造，它内部的 fd 是 -1，
        //      但 NodeLog 用的是自己的 m_fd，所以照样能落盘
        LogEntry raw;
        raw.set_term(9);
        raw.set_command(cmds[4]);
        log.appendLog(LogEntryWrapper(raw));
        check(log.persistLog(5) == m_sylar::IOState::SUCCESS,
              "G21. ★ entry 自带的 fd=-1 也能落盘（persistLog 用的是 NodeLog 自己的分片 fd）");
        co_await LogEntryWrapper::readHeader(fd, meta);
        check(meta.entry_count == 5, "G22. 文件头 entry_count == 5",
              "count=" + u64s(meta.entry_count));
        check(meta.base_idx == 1 && log.getBase() == 1, "G23. base_idx 全程没被改（仍是 1）");

        // G24. 落盘内容要和内存里的顺序完全一致
        std::vector<LogEntry> entries = co_await LogEntryWrapper::readLogFile(fd);
        bool same = entries.size() == 5;
        if (same) {
            for (int i = 0; i < 5; ++i) {
                if (entries[i].command() != cmds[i]) {
                    same = false;
                }
            }
        }
        check(same, "G24. ★ readLogFile 读回 5 条，顺序/内容与 append 顺序一致",
              "n=" + u64s(entries.size()));

        ::close(fd);
        ::unlink(shard.c_str());
    }

    co_return;
}


int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);

    g_path = "/tmp/kvraft_log_test_" + std::to_string(::getpid()) + ".log";
    std::printf("临时文件: %s\n", g_path.c_str());

    runAll();       // eager Task：体里全是同步完成的 co_await，会一路跑到 co_return

    ::unlink(g_path.c_str());
    std::printf("\n结果: pass=%d fail=%d\n", g_pass, g_fail);

    // 和别的测试一样用 _Exit：m_sylar 的静态对象析构在非 IOManager 线程上不安全
    std::_Exit(g_fail == 0 ? 0 : 1);
}
