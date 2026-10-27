#pragma once
#include "types.pb.h"
#include <memory.h>
#include <cstdint>
#include <deque>        // changed: 原来写的是 <queue>（容器实际是 deque）
#include <optional>     // changed: getTermAt 用 optional 表达"取不到"
#include <string>
#include <vector>
#include <coroutine/coro20/ioManager.h>
#include <sys/stat.h>
#include <unistd.h>
/**
 *   raft日志管理模块
*/


namespace craft {


// 日志元信息格式
// ---------------------------------------------------------------- 分片日志文件格式, 大端
//   文件头布局：
//     偏移   长度   字段          说明
//     0      4     magic         0x1A2B3C4D（非零：用来区分"本模块的段文件"和任意文件）
//     4      2     version       当前 1；读到不认识的版本就当作"不是分片文件"
//     6      2     header_size   文件头总长（当前 32），记录区从它开始 —— 以后加字段靠它兼容
//     8      8     base_idx      本文件【第一条】日志的全局索引（分片排序就靠它）
//     16     8     entry_count   本文件【当前】日志条数（每 append 一条就就地 +1）
//     24     8     reserved      预留（当前恒 0）：以后放校验和 / 末条 term 之类
//   记录区（从 header_size 开始，变长顺序排列）：
//     [ uint32 大端长度 ][ LogEntry 的 protobuf 字节 ] × N
//     所以"定位段内第 k 条"只能从段头顺序扫描 —— 靠段大小/条数阈值把扫描量压住

inline constexpr uint32_t kLogHeaderMagic      = 0x1A2B3C4D;
inline constexpr uint16_t kLogHeaderVersion    = 1;
inline constexpr uint32_t kLogHeaderSize       = 32;
inline constexpr uint32_t kLogEntryCountOffset = 16;             // entry_count 字段偏移（就地更新用）

//每段最多多少条日志
inline constexpr uint32_t kMaxRecordCount = 1024 * 10;
// 每段最多多少字节
inline constexpr uint32_t kLogSegmentMaxBytes = 8u * 1024 * 1024;
// 单条记录长度上限
inline constexpr uint32_t kMaxRecordBytes = 64u * 1024 * 1024;

// 文件头元信息
struct LogFileMeta {
    uint32_t header_size {kLogHeaderSize};      // 记录区起始偏移
    uint64_t base_idx {0};                      // 本文件第一条日志的全局索引
    uint64_t entry_count {0};                   // 本文件当前日志条数

    // 本文件最后一条日志的全局索引
    [[nodiscard]] uint64_t lastIdx() const { return base_idx + entry_count - 1; }
    [[nodiscard]] bool empty() const { return entry_count == 0; }
};

// 段文件命名{node_id}_{begin_idx}.seg
// 状态元信息
class StateMeta {
public:
    enum class State {
        Failed,     // 加载失败（文件存在但损坏 ⇒ 调用方必须拒绝启动）
        Success,    // 成功加载
        Create      // 未找到元信息，创建新的（全新节点：term=0, voted_for=-1）
    };

    m_sylar::Task<State> load(int id, const std::string& name = "state_meta.ras");         // 加载meta信息, 实际名称 {id}_{name}
    m_sylar::Task<m_sylar::IOState> save();     // 原子落盘：写 .tmp → fdatasync → rename → fsync(dir)

    void setCurrentTerm (const uint64_t current_term) {m_meta.set_current_term(current_term); }
    void setVotedFor (const int64_t voted_for) {m_meta.set_voted_for(voted_for); }
    void setCommitIdx (const uint64_t commit_idx) {m_meta.set_commit_idx(commit_idx); }
    void setAppliedIdx (const uint64_t applied_idx) {m_meta.set_applied_idx(applied_idx); }
    void setLastFileName (const std::string& name) {m_meta.set_last_log_file_name(name); }

    [[nodiscard]] uint64_t getCurrentTerm() const {return m_meta.current_term(); }
    [[nodiscard]] int64_t getVotedFor() const {return m_meta.voted_for(); }
    [[nodiscard]] uint64_t getCommitIdx() const {return m_meta.commit_idx(); }
    [[nodiscard]] uint64_t getAppliedIdx() const {return m_meta.applied_idx(); }
    [[nodiscard]] std::string getLastFileName() const {return m_meta.last_log_file_name(); }

private:
    PersistentState m_meta;
    std::string m_path;
};


class NodeLog {
public:
    using ptr = std::shared_ptr<NodeLog>;

    // changed: 需要 node_id 才能定位 {node_id}_{begin_idx}.seg（多节点同目录时互不干扰）
    m_sylar::Task<m_sylar::IOState> init(int node_id);                  // 初始化：扫段、修尾部、加载最后一段
    m_sylar::Task<m_sylar::IOState> coLoadLogSeg(uint64_t to_idx);      // 把内存窗口向前扩展到包含 to_idx
    m_sylar::Task<bool> coFreeLogSeg(uint64_t to_idx);                  // 释放内存窗口 [first_idx, to_idx]


    void appendLog(const LogEntry& entry);                                      // 添加日志, 推动log_idx
    // 日志复制: 持久(落盘)化索引idx的log,幂等, idx前必须全部落盘否则返回failed,若超过当前日志分片最大值，创建新分片，加载新fd。
    m_sylar::IOState persistLog(uint64_t idx);
    // 日志复制: 提交log(不落盘),需要确保可commit,idx前必须全部提交
    bool commitLog(uint64_t idx);               // 提交最后一条持久化为提交日志， 推动commit_idx, false：提交失败，true：成功或已经提交
    // 日志复制: 检查前一个位置是否符合term条件以新增一个log
    [[nodiscard]] bool matchLog(uint64_t prev_log_index, uint64_t prev_log_term) const;
    // 日志冲突: 从index处截断未提交日志，若日志已提交，返回失败。
    m_sylar::IOState truncateFrom(uint64_t index);

    std::optional<uint64_t> getTermAt(uint64_t idx);        // 获取索引日志的term（内存优先，窗口外读盘；取不到 nullopt）
    // 内存窗口内直接拷；窗口外按段顺序扫描读盘（不加载进 NodeLog）。idx > last ⇒ 空结果。
    std::deque<LogEntry> getEntriesFrom(int idx, int32_t count);


    [[nodiscard]] uint64_t getFirstIdx() const { return m_first_idx; }      // changed: 内存窗口起点
    [[nodiscard]] uint64_t getLastLogIdx() const { return m_last_log_idx; }
    [[nodiscard]] uint64_t getLastLogTerm() const { return m_last_log_term; }
    [[nodiscard]] uint64_t getCommitIndex() const {return m_commit_idx;}

    [[nodiscard]] uint64_t getPersistIndex() const { return m_persist_idx; }
    [[nodiscard]] int getFd() const { return m_fd; }

protected:
    void setCommitIndex(const uint64_t idx) {m_commit_idx = idx; }

private:
    // changed: 段表 + 段内读写工具
    struct SegInfo {
        uint64_t base_idx {0};                  // 本段第一条日志的全局索引
        uint64_t entry_count {0};               // 本段有效条数（读出来并修复过尾部）
        std::string path;                       // 段文件路径
        [[nodiscard]] uint64_t lastIdx() const { return base_idx + entry_count - 1; }
        [[nodiscard]] bool empty() const { return entry_count == 0; }
    };

    [[nodiscard]] std::string segPath(uint64_t base_idx) const;
    [[nodiscard]] const SegInfo* findSeg(uint64_t idx) const;
    bool readSegEntries(const SegInfo& seg, std::vector<LogEntry>& out, uint64_t& valid) const;
    bool readEntryAt(uint64_t idx, LogEntry& out) const;
    bool lookupTerm(uint64_t idx, uint64_t& term) const;            // changed: 供 const 的 matchLog 用
    m_sylar::IOState ensureWriteFd();
    m_sylar::IOState createSeg(uint64_t base_idx);
    m_sylar::IOState flushWriteSeg(bool sync_data);

    int m_node_id {-1};             // changed: 段文件命名用
    uint64_t m_first_idx {1};       // changed: 内存窗口第一条日志的全局索引（空日志时 last < first）
    int m_fd {-1};                  // changed: 原来是 vector<int>，但 getFd() 返回 int；当前只保留写入段 fd
    std::vector<SegInfo> m_segs;    // changed: 段表，按 base_idx 升序

    // 日志信息管理
    std::deque<LogEntry> m_log_entries;
    uint64_t m_last_log_term {0};                                       // 全局最后一条日志的 term
    uint64_t m_last_log_idx {0};                                        // 全局最后一条日志的索引（绝对）
    uint64_t m_persist_idx {0};                                        // 已落盘位点：<= 它的日志都已 fsync
    uint64_t m_commit_idx{0};                                         // 已提交位点（仅内存，重启由 leader 重新告知）
};

}
