#pragma once
#include "types.pb.h"
#include <memory.h>
#include <cstdint>
#include <coroutine/coro20/ioManager.h>
#include <sys/stat.h>
#include <unistd.h>
/**
 *   raft日志管理模块
*/


namespace craft {

//   文件格式
//   原来 LogEntry 里没有 index 字段、文件里也没有任何元信息，分片文件无法排序。
//   现在每个分片文件开头写 32 字节文件头，用 base_idx 记"本片第一条日志的全局索引"、
//   用 entry_count 记"本片当前条数"，全大端（和记录长度头一致）。
// ---------------------------------------------------------------- 分片日志文件格式, 大端
//   文件头布局：
//     偏移   长度   字段          说明
//     0      4     magic         0x4B564C47 = 'K''V''L''G'，用来把自己和裸记录老文件区分开
//     4      2     version       当前 1；读到不认识的版本就当作"不是分片文件"
//     6      2     header_size   文件头总长（当前 32），记录区从它开始 —— 以后加字段靠它兼容
//     8      8     base_idx      本文件【第一条】日志的全局索引（分片排序就靠它）
//     16     8     entry_count   本文件【当前】日志条数（每 append 一条就就地 +1）
//     24     8     reserved      预留（当前恒 0）：以后放校验和 / 末条 term 之类

inline constexpr uint32_t kLogHeaderMagic      = 0x4B564C47;      // 'KVLG'
inline constexpr uint16_t kLogHeaderVersion    = 1;
inline constexpr uint32_t kLogHeaderSize       = 32;
inline constexpr uint32_t kLogEntryCountOffset = 16;             // entry_count 字段偏移（就地更新用）

// 单条日志记录的长度上限
// 和 RPC 层 kMaxPayloadSize 同量级：command 就是一次 KV 请求的字节。
inline constexpr uint32_t kMaxRecordSize = 64u * 1024 * 1024;

// 文件头元信息
struct LogFileMeta {
    uint32_t header_size {kLogHeaderSize};      // 记录区起始偏移
    uint64_t base_idx {0};                      // 本文件第一条日志的全局索引
    uint64_t entry_count {0};                   // 本文件当前日志条数

    // 本文件最后一条日志的全局索引
    [[nodiscard]] uint64_t lastIdx() const { return base_idx + entry_count - 1; }
    [[nodiscard]] bool empty() const { return entry_count == 0; }
};


class LogEntryWrapper{
public:
    explicit LogEntryWrapper(const LogEntry& other) : m_entry(other) {}
    explicit LogEntryWrapper(LogEntry&& other) : m_entry(std::move(other)) {}
    explicit LogEntryWrapper(uint64_t term, const std::string& command, int fd) : m_fd(fd) {
        m_entry.set_term(term);
        m_entry.set_command(command);
    }


    // 落盘函数
    m_sylar::Task<bool> persist() const;        // 追加当前entry（成功后会把文件头 entry_count +1）
    // 读取函数
    static m_sylar::Task<std::vector<LogEntry>> readLogFile(int fd);        // 读取一个log文件的所有entry


    // log文件创建
    static m_sylar::Task<bool> create(int fd, uint64_t base_idx);
    // 读文件头
    static m_sylar::Task<bool> readHeader(int fd, LogFileMeta& meta);
    // 就地改写文件头里的 entry_count
    static m_sylar::Task<bool> updateEntryCount(int fd, uint64_t entry_count);


    [[nodiscard]] const LogEntry& getEntry() const {return m_entry;}
private:
    static m_sylar::Task<bool> writeAll(int fd, const void* buffer, size_t size);
    static m_sylar::Task<void> readAllAt(int fd, void* buffer, size_t size, uint64_t offset);


private:
    LogEntry m_entry;
    int m_fd {-1};
};


class NodeLog {
public:
    using ptr = std::shared_ptr<NodeLog>;

    void appendLog(const LogEntryWrapper& entry);                                      // 添加日志, 推动log_idx
    // 持久化化idx索引log,幂等, idx前必须全部落盘否则返回failed
    m_sylar::IOState persistLog(uint64_t idx);
    // 提交log(不落盘),需要确保可commit,idx前必须全部提交
    bool commitLog(uint64_t idx);               // 提交最后一条持久化为提交日志， 推动commit_idx, false：提交失败，true：成功或已经提交



    // 信息处理
    void setCommitIndex(const uint64_t idx) {m_commit_idx = idx; }

    bool matchLog(uint64_t prev_log_index, uint64_t prev_log_term) const;

    [[nodiscard]] uint64_t getLastLogIdx() const { return m_last_log_idx; }
    [[nodiscard]] uint64_t getLastLogTerm() const { return m_last_log_term; }
    [[nodiscard]] uint64_t getCommitIndex() const {return m_commit_idx;}

    [[nodiscard]] uint64_t getPersistIndex() const { return m_persist_idx; }
    void attach(int fd, uint64_t base_idx) {
        m_fd = fd;
        m_base = base_idx;
    }
    [[nodiscard]] int getFd() const { return m_fd; }
    [[nodiscard]] uint64_t getBase() const { return m_base; }
private:
    uint64_t m_base{0};     // 基日志idx
    int m_fd {-1};          // log fd

    // 日志信息管理
    std::vector<LogEntryWrapper> m_log_entries;
    uint64_t m_last_log_term {0};                                       // 最新未提交日志term
    uint64_t m_last_log_idx {0};                                        // 最新未提交日志相对idx
    uint64_t m_persist_idx {0};                                        // 最新未落盘
    uint64_t m_commit_idx{0};                                         // 最新已提交日志相对idx
};

}
