#include "log.hpp"

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <dirent.h>
#include <fcntl.h>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

#include <basic/log.h>


namespace craft {

static auto g_logger = M_SYLAR_LOG_NAME("craft");

// ------------------------------------------------------------------ 大端编解码
// 手写大端：不依赖 htobe64 之类的 <endian.h> 扩展宏（严格 -std=c++20 下会被宏藏起来）
static void putU16(char* p, uint16_t v) {
    p[0] = static_cast<char>((v >> 8) & 0xFF);
    p[1] = static_cast<char>(v & 0xFF);
}

static void putU32(char* p, uint32_t v) {
    p[0] = static_cast<char>((v >> 24) & 0xFF);
    p[1] = static_cast<char>((v >> 16) & 0xFF);
    p[2] = static_cast<char>((v >> 8) & 0xFF);
    p[3] = static_cast<char>(v & 0xFF);
}

static void putU64(char* p, uint64_t v) {
    for (int i = 7; i >= 0; --i) {
        p[i] = static_cast<char>(v & 0xFF);
        v >>= 8;
    }
}

static uint16_t getU16(const char* p) {
    return static_cast<uint16_t>((static_cast<uint16_t>(static_cast<uint8_t>(p[0])) << 8) |
                                 static_cast<uint16_t>(static_cast<uint8_t>(p[1])));
}

static uint32_t getU32(const char* p) {
    uint32_t v = 0;
    for (int i = 0; i < 4; ++i) {
        v = (v << 8) | static_cast<uint8_t>(p[i]);
    }
    return v;
}

static uint64_t getU64(const char* p) {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) {
        v = (v << 8) | static_cast<uint8_t>(p[i]);
    }
    return v;
}

// ------------------------------------------------------------------ 文件小工具
static bool writeAll(int fd, const void* buffer, size_t size) {
    const auto* p = static_cast<const char*>(buffer);
    size_t done = 0;
    while (done < size) {
        const ssize_t n = ::write(fd, p + done, size - done);
        if (n < 0) {
            if (errno == EINTR) { continue; }
            return false;
        }
        if (n == 0) { return false; }
        done += static_cast<size_t>(n);
    }
    return true;
}

static bool readExactAt(int fd, void* buffer, size_t size, off_t offset) {
    auto* p = static_cast<char*>(buffer);
    size_t done = 0;
    while (done < size) {
        const ssize_t n = ::pread(fd, p + done, size - done, offset + static_cast<off_t>(done));
        if (n < 0) {
            if (errno == EINTR) { continue; }
            return false;
        }
        if (n == 0) { return false; }       // 提前 EOF：记录被截断
        done += static_cast<size_t>(n);
    }
    return true;
}

static bool pwriteAll(int fd, const void* buffer, size_t size, off_t offset) {
    const auto* p = static_cast<const char*>(buffer);
    size_t done = 0;
    while (done < size) {
        const ssize_t n = ::pwrite(fd, p + done, size - done, offset + static_cast<off_t>(done));
        if (n < 0) {
            if (errno == EINTR) { continue; }
            return false;
        }
        if (n == 0) { return false; }
        done += static_cast<size_t>(n);
    }
    return true;
}

static bool fsyncDirOf(const std::string& path) {
    const size_t slash = path.find_last_of('/');
    const std::string dir = (slash == std::string::npos) ? std::string(".") : path.substr(0, slash);
    const int fd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY);
    if (fd < 0) { return false; }
    const bool ok = (::fsync(fd) == 0);
    ::close(fd);
    return ok;
}

static uint32_t crc32(const void* data, size_t size) {
    const auto* p = static_cast<const uint8_t*>(data);
    uint32_t crc = 0xFFFFFFFFu;
    for (size_t i = 0; i < size; ++i) {
        crc ^= p[i];
        for (int k = 0; k < 8; ++k) {
            crc = (crc & 1u) ? ((crc >> 1) ^ 0xEDB88320u) : (crc >> 1);
        }
    }
    return ~crc;
}

static std::string fileNameOf(const std::string& path) {
    const size_t slash = path.find_last_of('/');
    return (slash == std::string::npos) ? path : path.substr(slash + 1);
}

// ------------------------------------------------------------------ 状态元信息
// 文件格式（大端）：
//   [magic u32 'STET'][version u16][payload_len u32][PersistentState 序列化字节][crc32(payload) u32]
static constexpr uint32_t kStateMagic = 0x53544554;      // 'STET'
static constexpr uint16_t kStateVersion = 1;

static void encodeState(const PersistentState& st, std::string& out) {
    const std::string payload = st.SerializeAsString();
    out.assign(kLogHeaderSize, '\0');                    // 用固定 16 字节头（复用常量，语义独立）
    char* p = out.data();
    putU32(p + 0, kStateMagic);
    putU16(p + 4, kStateVersion);
    putU32(p + 6, static_cast<uint32_t>(payload.size()));
    out.resize(16);
    out.append(payload);
    char crc_buf[4];
    putU32(crc_buf, crc32(payload.data(), payload.size()));
    out.append(crc_buf, 4);
}

static bool decodeState(const std::string& in, PersistentState& out) {
    if (in.size() < 16 + 4) { return false; }
    if (getU32(in.data() + 0) != kStateMagic) { return false; }
    if (getU16(in.data() + 4) != kStateVersion) { return false; }
    const uint32_t len = getU32(in.data() + 6);
    if (in.size() != 16u + len + 4u) { return false; }
    const char* payload = in.data() + 16;
    const uint32_t expect = getU32(in.data() + 16 + len);
    if (crc32(payload, len) != expect) { return false; }
    if (!out.ParseFromString(std::string(payload, len))) { return false; }
    return true;
}

m_sylar::Task<StateMeta::State> StateMeta::load(int id, const std::string& name) {
    m_path = std::to_string(id) + "_" + name;
    m_meta.set_meta_name(m_path);

    const int fd = ::open(m_path.c_str(), O_RDONLY);
    if (fd < 0) {
        if (errno == ENOENT) {
            // 全新节点：term=0、未投票（用 -1，因为 0 是合法节点 id）
            m_meta.set_current_term(0);
            m_meta.set_voted_for(-1);
            m_meta.set_commit_idx(0);
            m_meta.set_applied_idx(0);
            m_meta.set_last_log_file_name("");
            co_return State::Create;
        }
        M_SYLAR_LOG_ERROR(g_logger) << "[log] 打开状态文件失败: " << m_path
                                    << " errno=" << errno << "(" << strerror(errno) << ")";
        co_return State::Failed;
    }

    struct stat st{};
    if (::fstat(fd, &st) < 0 || st.st_size <= 0) {
        ::close(fd);
        M_SYLAR_LOG_ERROR(g_logger) << "[log] 状态文件非法（空或 fstat 失败）: " << m_path;
        co_return State::Failed;
    }

    std::string buf(static_cast<size_t>(st.st_size), '\0');
    const bool ok = readExactAt(fd, buf.data(), buf.size(), 0);
    ::close(fd);
    if (!ok) {
        M_SYLAR_LOG_ERROR(g_logger) << "[log] 读取状态文件失败: " << m_path;
        co_return State::Failed;
    }

    if (!decodeState(buf, m_meta)) {
        // 文件存在但损坏：必须让上层拒绝启动（不能当作全新节点，否则可能同任期重复投票）
        M_SYLAR_LOG_ERROR(g_logger) << "[log] 状态文件校验失败（magic/version/长度/CRC）: " << m_path;
        co_return State::Failed;
    }
    if (m_meta.voted_for() == 0) {
        // proto3 里 int64 无 presence：0 要么是真投给节点 0，要么是旧文件没写。这里按"投给节点 0"处理；
        // 由 create 分支保证新文件写 -1，所以正常不会出现歧义。
    }
    co_return State::Success;
}

m_sylar::Task<m_sylar::IOState> StateMeta::save() {
    std::string path = m_path;
    if (path.empty()) { path = m_meta.meta_name(); }
    if (path.empty()) {
        M_SYLAR_LOG_ERROR(g_logger) << "[log] 保存状态失败：路径未知（先调用 load）";
        co_return m_sylar::IOState::FAILED;
    }

    std::string buf;
    encodeState(m_meta, buf);

    const std::string tmp = path + ".tmp";
    const int fd = ::open(tmp.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
    if (fd < 0) {
        M_SYLAR_LOG_ERROR(g_logger) << "[log] 创建临时状态文件失败: " << tmp;
        co_return m_sylar::IOState::FAILED;
    }
    if (!writeAll(fd, buf.data(), buf.size())) {
        ::close(fd);
        ::unlink(tmp.c_str());
        co_return m_sylar::IOState::FAILED;
    }
    if (::fdatasync(fd) < 0) {
        ::close(fd);
        ::unlink(tmp.c_str());
        co_return m_sylar::IOState::FAILED;
    }
    ::close(fd);

    if (::rename(tmp.c_str(), path.c_str()) < 0) {      // rename 原子替换
        ::unlink(tmp.c_str());
        co_return m_sylar::IOState::FAILED;
    }
    fsyncDirOf(path);                                   // 让 rename 本身持久化
    co_return m_sylar::IOState::SUCCESS;
}


// ------------------------------------------------------------------ NodeLog：段文件
std::string NodeLog::segPath(uint64_t base_idx) const {
    return std::to_string(m_node_id) + "_" + std::to_string(base_idx) + ".seg";
}

const NodeLog::SegInfo* NodeLog::findSeg(uint64_t idx) const {
    // m_segs 按 base_idx 升序：找最后一个 base_idx <= idx 的段
    if (m_segs.empty()) { return nullptr; }
    if (idx < m_segs.front().base_idx) { return nullptr; }
    size_t lo = 0, hi = m_segs.size();                  // 第一个 base_idx > idx 的位置
    while (lo < hi) {
        const size_t mid = (lo + hi) / 2;
        if (m_segs[mid].base_idx <= idx) { lo = mid + 1; } else { hi = mid; }
    }
    if (lo == 0) { return nullptr; }
    const SegInfo& s = m_segs[lo - 1];
    if (idx > s.lastIdx()) { return nullptr; }
    return &s;
}

// 顺序扫描一个段，读出全部能解析的记录。valid = 实际解析出的条数（用于修复崩溃尾部）
bool NodeLog::readSegEntries(const SegInfo& seg, std::vector<LogEntry>& out, uint64_t& valid) const {
    out.clear();
    valid = 0;

    const int fd = ::open(seg.path.c_str(), O_RDONLY);
    if (fd < 0) {
        M_SYLAR_LOG_ERROR(g_logger) << "[log] 打开段文件失败: " << seg.path;
        return false;
    }

    struct stat st{};
    if (::fstat(fd, &st) < 0 || st.st_size < static_cast<off_t>(kLogHeaderSize)) {
        ::close(fd);
        M_SYLAR_LOG_ERROR(g_logger) << "[log] 段文件过短/无法 stat: " << seg.path;
        return false;
    }
    const uint64_t file_size = static_cast<uint64_t>(st.st_size);

    char header[kLogHeaderSize] = {0};
    if (!readExactAt(fd, header, sizeof(header), 0)) {
        ::close(fd);
        return false;
    }
    const uint32_t magic = getU32(header + 0);
    const uint16_t version = getU16(header + 4);
    const uint16_t header_size = getU16(header + 6);
    if (magic != kLogHeaderMagic || version != kLogHeaderVersion ||
        header_size < kLogHeaderSize || header_size > file_size) {
        ::close(fd);
        M_SYLAR_LOG_ERROR(g_logger) << "[log] 段文件头非法: " << seg.path
                                    << " magic=" << magic << " version=" << version
                                    << " header_size=" << header_size;
        return false;
    }

    uint64_t offset = header_size;
    while (offset + sizeof(uint32_t) <= file_size) {
        uint32_t net_len = 0;
        if (!readExactAt(fd, &net_len, sizeof(net_len), static_cast<off_t>(offset))) { break; }
        const uint32_t len = getU32(reinterpret_cast<const char*>(&net_len));
        if (len == 0 || len > kMaxRecordBytes) { break; }
        if (offset + sizeof(uint32_t) + len > file_size) { break; }     // 尾部半截记录

        std::string payload(len, '\0');
        if (!readExactAt(fd, payload.data(), payload.size(),
                         static_cast<off_t>(offset + sizeof(uint32_t)))) {
            break;
        }
        LogEntry entry;
        if (!entry.ParseFromString(payload)) { break; }
        out.push_back(std::move(entry));
        offset += sizeof(uint32_t) + len;
        ++valid;
    }

    ::close(fd);
    return true;
}

// 读单个索引对应的日志（内存里没有时用），顺序扫描段文件
bool NodeLog::readEntryAt(uint64_t idx, LogEntry& out) const {
    if (idx == 0) {
        out.Clear();
        return true;
    }
    const SegInfo* seg = findSeg(idx);
    if (seg == nullptr) { return false; }

    std::vector<LogEntry> entries;
    uint64_t valid = 0;
    if (!readSegEntries(*seg, entries, valid)) { return false; }
    if (idx < seg->base_idx || idx - seg->base_idx >= entries.size()) { return false; }
    out = entries[idx - seg->base_idx];
    return true;
}

bool NodeLog::lookupTerm(uint64_t idx, uint64_t& term) const {
    if (idx == 0) {
        term = 0;
        return true;
    }
    if (idx > m_last_log_idx) { return false; }
    if (idx >= m_first_idx && idx - m_first_idx < m_log_entries.size()) {
        term = m_log_entries[idx - m_first_idx].term();
        return true;
    }
    LogEntry entry;
    if (!readEntryAt(idx, entry)) { return false; }
    term = entry.term();
    return true;
}

m_sylar::IOState NodeLog::createSeg(uint64_t base_idx) {
    const std::string path = segPath(base_idx);
    const int fd = ::open(path.c_str(), O_CREAT | O_TRUNC | O_RDWR, 0644);
    if (fd < 0) {
        M_SYLAR_LOG_ERROR(g_logger) << "[log] 创建段文件失败: " << path;
        return m_sylar::IOState::FAILED;
    }

    char header[kLogHeaderSize] = {0};
    putU32(header + 0, kLogHeaderMagic);
    putU16(header + 4, kLogHeaderVersion);
    putU16(header + 6, static_cast<uint16_t>(kLogHeaderSize));
    putU64(header + 8, base_idx);
    putU64(header + 16, 0);                             // entry_count 从 0 开始

    if (!writeAll(fd, header, sizeof(header)) || ::fdatasync(fd) < 0) {
        ::close(fd);
        ::unlink(path.c_str());
        return m_sylar::IOState::FAILED;
    }
    fsyncDirOf(path);

    if (m_fd >= 0) { ::close(m_fd); }
    m_fd = fd;
    m_segs.push_back(SegInfo{base_idx, 0, path});
    M_SYLAR_LOG_INFO(g_logger) << "[log] 新建段: " << path << " base=" << base_idx;
    return m_sylar::IOState::SUCCESS;
}

m_sylar::IOState NodeLog::ensureWriteFd() {
    if (m_fd >= 0) { return m_sylar::IOState::SUCCESS; }
    if (m_segs.empty()) {
        return createSeg(m_persist_idx + 1);            // 还没有任何段：正好从下一条要落盘的索引开始
    }
    SegInfo& s = m_segs.back();
    const int fd = ::open(s.path.c_str(), O_RDWR);
    if (fd < 0) {
        M_SYLAR_LOG_ERROR(g_logger) << "[log] 重开写入段失败: " << s.path;
        return m_sylar::IOState::FAILED;
    }
    if (::lseek(fd, 0, SEEK_END) < 0) {
        ::close(fd);
        return m_sylar::IOState::FAILED;
    }
    m_fd = fd;
    return m_sylar::IOState::SUCCESS;
}

m_sylar::IOState NodeLog::flushWriteSeg(bool sync_data) {
    if (m_fd < 0 || m_segs.empty()) { return m_sylar::IOState::SUCCESS; }
    char buf[8] = {0};
    putU64(buf, m_segs.back().entry_count);
    if (!pwriteAll(m_fd, buf, sizeof(buf), static_cast<off_t>(kLogEntryCountOffset))) {
        return m_sylar::IOState::FAILED;
    }
    if (sync_data && ::fdatasync(m_fd) < 0) {
        return m_sylar::IOState::FAILED;
    }
    return m_sylar::IOState::SUCCESS;
}

// ------------------------------------------------------------------ NodeLog：初始化/恢复
m_sylar::Task<m_sylar::IOState> NodeLog::init(int node_id) {
    m_node_id = node_id;
    m_segs.clear();
    m_log_entries.clear();
    if (m_fd >= 0) { ::close(m_fd); m_fd = -1; }

    m_first_idx = 1;
    m_last_log_idx = 0;
    m_last_log_term = 0;
    m_persist_idx = 0;
    m_commit_idx = 0;

    // 1) 扫当前目录，找出 {node_id}_{begin}.seg
    const std::string prefix = std::to_string(node_id) + "_";
    const std::string suffix = ".seg";

    std::vector<std::pair<uint64_t, std::string>> found;
    DIR* dir = ::opendir(".");
    if (dir == nullptr) {
        M_SYLAR_LOG_ERROR(g_logger) << "[log] opendir(\".\") 失败";
        co_return m_sylar::IOState::FAILED;
    }
    while (struct dirent* ent = ::readdir(dir)) {
        const std::string name = ent->d_name;
        if (name.size() <= prefix.size() + suffix.size()) { continue; }
        if (name.compare(0, prefix.size(), prefix) != 0) { continue; }
        if (name.compare(name.size() - suffix.size(), suffix.size(), suffix) != 0) { continue; }
        const std::string num = name.substr(prefix.size(), name.size() - prefix.size() - suffix.size());
        if (num.empty() || num.find_first_not_of("0123456789") != std::string::npos) { continue; }
        found.emplace_back(static_cast<uint64_t>(std::strtoull(num.c_str(), nullptr, 10)), name);
    }
    ::closedir(dir);
    std::sort(found.begin(), found.end());

    // 2) 逐段校验 + 修复尾部（崩溃可能把最后一条记录写成半截）
    for (const auto& item : found) {
        const uint64_t base = item.first;
        const std::string path = item.second;

        SegInfo seg{base, 0, path};
        std::vector<LogEntry> entries;
        uint64_t valid = 0;
        if (!readSegEntries(seg, entries, valid)) {
            M_SYLAR_LOG_ERROR(g_logger) << "[log] 段文件损坏，拒绝启动: " << path;
            co_return m_sylar::IOState::FAILED;
        }

        // 头里的 base 必须和文件名一致
        {
            const int fd = ::open(path.c_str(), O_RDONLY);
            char header[kLogHeaderSize] = {0};
            const bool ok = (fd >= 0) && readExactAt(fd, header, sizeof(header), 0);
            const uint64_t head_base = ok ? getU64(header + 8) : 0;
            const uint64_t head_count = ok ? getU64(header + 16) : 0;
            if (fd >= 0) { ::close(fd); }
            if (!ok || head_base != base) {
                M_SYLAR_LOG_ERROR(g_logger) << "[log] 段头与文件名不一致: " << path
                                            << " head_base=" << head_base << " name_base=" << base;
                co_return m_sylar::IOState::FAILED;
            }
            if (valid != head_count) {
                // 尾部修复：截断到实际解析出的记录末尾，并改写头里的 entry_count
                M_SYLAR_LOG_WARN(g_logger) << "[log] 修复段尾部: " << path
                                           << " header_count=" << head_count << " valid=" << valid;
                const int wfd = ::open(path.c_str(), O_RDWR);
                if (wfd < 0) { co_return m_sylar::IOState::FAILED; }
                // 重新算有效数据末尾
                off_t end = static_cast<off_t>(kLogHeaderSize);
                for (uint64_t k = 0; k < valid; ++k) {
                    end += static_cast<off_t>(sizeof(uint32_t)) + entries[k].ByteSizeLong();
                }
                char cnt[8] = {0};
                putU64(cnt, valid);
                const bool trim_ok = ::ftruncate(wfd, end) == 0 &&
                                     pwriteAll(wfd, cnt, sizeof(cnt),
                                               static_cast<off_t>(kLogEntryCountOffset)) &&
                                     ::fdatasync(wfd) == 0;
                ::close(wfd);
                if (!trim_ok) {
                    M_SYLAR_LOG_ERROR(g_logger) << "[log] 修复段尾部失败: " << path;
                    co_return m_sylar::IOState::FAILED;
                }
            }
        }

        seg.entry_count = valid;
        m_segs.push_back(seg);
    }

    // 3) 段之间必须严格连续
    for (size_t i = 1; i < m_segs.size(); ++i) {
        if (m_segs[i].base_idx != m_segs[i - 1].lastIdx() + 1) {
            M_SYLAR_LOG_ERROR(g_logger) << "[log] 段不连续: " << m_segs[i - 1].path
                                        << "(last=" << m_segs[i - 1].lastIdx() << ") -> "
                                        << m_segs[i].path << "(base=" << m_segs[i].base_idx << ")";
            co_return m_sylar::IOState::FAILED;
        }
    }

    // 4) 丢掉尾部空段（避免 last = base-1 的边界，让 last 总有意义）
    while (!m_segs.empty() && m_segs.back().entry_count == 0) {
        ::unlink(m_segs.back().path.c_str());
        m_segs.pop_back();
    }

    if (m_segs.empty()) {
        M_SYLAR_LOG_INFO(g_logger) << "[log] node " << node_id << " 无历史日志，从索引 1 开始";
        m_first_idx = 1;
        m_last_log_idx = 0;
        m_last_log_term = 0;
        m_persist_idx = 0;
        co_return m_sylar::IOState::SUCCESS;
    }

    // 5) 只把最后一段加载进内存；更早的按需从磁盘读（getEntriesFrom / coLoadLogSeg）
    SegInfo& last = m_segs.back();
    std::vector<LogEntry> entries;
    uint64_t valid = 0;
    if (!readSegEntries(last, entries, valid) || valid != last.entry_count) {
        M_SYLAR_LOG_ERROR(g_logger) << "[log] 加载最后一段失败: " << last.path;
        co_return m_sylar::IOState::FAILED;
    }
    m_log_entries.assign(entries.begin(), entries.end());
    m_first_idx = last.base_idx;
    m_last_log_idx = last.lastIdx();
    m_last_log_term = m_log_entries.empty() ? 0 : m_log_entries.back().term();
    m_persist_idx = m_last_log_idx;                     // 磁盘上已有的都算已落盘

    if (ensureWriteFd() != m_sylar::IOState::SUCCESS) {
        co_return m_sylar::IOState::FAILED;
    }

    M_SYLAR_LOG_INFO(g_logger) << "[log] node " << node_id << " 恢复完成: segs=" << m_segs.size()
                               << " mem=[" << m_first_idx << "," << m_last_log_idx << "]"
                               << " last_term=" << m_last_log_term;
    co_return m_sylar::IOState::SUCCESS;
}

// ------------------------------------------------------------------ NodeLog：写入
void NodeLog::appendLog(const LogEntry& entry) {
    if (m_log_entries.empty()) {
        m_first_idx = m_last_log_idx + 1;               // 空窗口：从下一条开始
    }
    m_log_entries.push_back(entry);
    ++m_last_log_idx;
    m_last_log_term = entry.term();
}

m_sylar::IOState NodeLog::persistLog(uint64_t idx) {
    if (idx > m_last_log_idx) {
        M_SYLAR_LOG_ERROR(g_logger) << "[log] persistLog(" << idx << ") 超过 last=" << m_last_log_idx;
        return m_sylar::IOState::FAILED;
    }
    if (idx <= m_persist_idx) {
        return m_sylar::IOState::SUCCESS;               // 幂等
    }
    if (ensureWriteFd() != m_sylar::IOState::SUCCESS) {
        return m_sylar::IOState::FAILED;
    }

    // 本轮要写 (m_persist_idx, idx]
    for (uint64_t i = m_persist_idx + 1; i <= idx; ++i) {
        if (i < m_first_idx || i - m_first_idx >= m_log_entries.size()) {
            M_SYLAR_LOG_ERROR(g_logger) << "[log] persistLog: 索引 " << i << " 不在内存窗口 ["
                                        << m_first_idx << ","
                                        << (m_first_idx + m_log_entries.size()) << ")";
            return m_sylar::IOState::FAILED;
        }
        const LogEntry& entry = m_log_entries[i - m_first_idx];

        // 按字节/条数切段：先把当前段收尾，再开新段（新段 base = 当前索引）
        struct stat st{};
        const bool need_rotate = (::fstat(m_fd, &st) == 0 &&
                                  static_cast<uint64_t>(st.st_size) >= kLogSegmentMaxBytes) ||
                                 (m_segs.back().entry_count >= kMaxRecordCount);
        if (need_rotate) {
            if (flushWriteSeg(true) != m_sylar::IOState::SUCCESS) { return m_sylar::IOState::FAILED; }
            if (createSeg(i) != m_sylar::IOState::SUCCESS) { return m_sylar::IOState::FAILED; }
        }

        const std::string payload = entry.SerializeAsString();
        if (payload.size() > kMaxRecordBytes) {
            M_SYLAR_LOG_ERROR(g_logger) << "[log] 单条日志过大: " << payload.size();
            return m_sylar::IOState::FAILED;
        }
        char len_buf[4] = {0};
        putU32(len_buf, static_cast<uint32_t>(payload.size()));
        if (!writeAll(m_fd, len_buf, sizeof(len_buf)) ||
            !writeAll(m_fd, payload.data(), payload.size())) {
            M_SYLAR_LOG_ERROR(g_logger) << "[log] 写段文件失败: " << m_segs.back().path
                                        << " errno=" << errno;
            return m_sylar::IOState::FAILED;
        }
        ++m_segs.back().entry_count;
    }

    if (flushWriteSeg(true) != m_sylar::IOState::SUCCESS) {
        return m_sylar::IOState::FAILED;
    }
    m_persist_idx = idx;
    return m_sylar::IOState::SUCCESS;
}

bool NodeLog::commitLog(uint64_t idx) {
    if (idx > m_last_log_idx) { return false; }         // 没有这条日志
    if (idx > m_persist_idx) { return false; }          // 没落盘的日志不许提交
    if (idx <= m_commit_idx) { return true; }           // 已经提交过
    m_commit_idx = idx;
    return true;
}

// ------------------------------------------------------------------ NodeLog：查询
std::optional<uint64_t> NodeLog::getTermAt(uint64_t idx) {
    uint64_t term = 0;
    if (!lookupTerm(idx, term)) {
        return std::nullopt;
    }
    return term;
}

bool NodeLog::matchLog(uint64_t prev_log_index, uint64_t prev_log_term) const {
    if (prev_log_index == 0) {
        return true;                                    // 哨兵：空前缀永远匹配
    }
    if (prev_log_index > m_last_log_idx) {
        return false;                                   // 本地没有那么长
    }
    uint64_t term = 0;
    if (!lookupTerm(prev_log_index, term)) {
        return false;                                   // 索引已被裁掉/读不到 ⇒ 无法校验 ⇒ 拒绝
    }
    return term == prev_log_term;
}

std::deque<LogEntry> NodeLog::getEntriesFrom(int idx, int32_t count) {
    std::deque<LogEntry> result;
    if (count <= 0 || idx <= 0) { return result; }
    const uint64_t begin = static_cast<uint64_t>(idx);
    if (begin > m_last_log_idx) { return result; }      // 没有这些日志（正常）

    uint64_t end = begin + static_cast<uint64_t>(count) - 1;
    if (end > m_last_log_idx) { end = m_last_log_idx; }

    uint64_t cur = begin;
    while (cur <= end) {
        // 1) 内存窗口内：直接拷
        if (cur >= m_first_idx && cur - m_first_idx < m_log_entries.size()) {
            const uint64_t available = m_log_entries.size() - (cur - m_first_idx);
            uint64_t n = end - cur + 1;
            if (n > available) { n = available; }
            for (uint64_t k = 0; k < n; ++k) {
                result.push_back(m_log_entries[cur - m_first_idx + k]);
            }
            cur += n;
            continue;
        }

        // 2) 窗口外：按段读盘（不进 NodeLog 内存）
        const SegInfo* seg = findSeg(cur);
        if (seg == nullptr) {
            M_SYLAR_LOG_ERROR(g_logger) << "[log] getEntriesFrom: 索引 " << cur << " 找不到所属段";
            break;
        }
        std::vector<LogEntry> seg_entries;
        uint64_t valid = 0;
        if (!readSegEntries(*seg, seg_entries, valid)) {
            M_SYLAR_LOG_ERROR(g_logger) << "[log] getEntriesFrom: 读段失败 " << seg->path;
            break;
        }
        uint64_t seg_end = end;
        if (seg_end > seg->base_idx + seg_entries.size() - 1) {
            seg_end = seg->base_idx + seg_entries.size() - 1;
        }
        for (uint64_t i = cur; i <= seg_end; ++i) {
            result.push_back(seg_entries[i - seg->base_idx]);
        }
        cur = seg_end + 1;
    }
    return result;
}

// ------------------------------------------------------------------ NodeLog：截断
m_sylar::IOState NodeLog::truncateFrom(uint64_t index) {
    if (index == 0 || index > m_last_log_idx) {
        return m_sylar::IOState::SUCCESS;               // 没有要截断的
    }
    if (index <= m_commit_idx) {
        M_SYLAR_LOG_ERROR(g_logger) << "[log] 拒绝截断已提交日志: index=" << index
                                    << " commit=" << m_commit_idx;
        return m_sylar::IOState::FAILED;
    }

    // 1) 内存窗口
    if (index <= m_first_idx) {
        m_log_entries.clear();
    } else {
        const size_t pos = static_cast<size_t>(index - m_first_idx);
        if (pos < m_log_entries.size()) {
            m_log_entries.erase(m_log_entries.begin() + static_cast<long>(pos), m_log_entries.end());
        }
    }

    // 2) 磁盘：先关写入 fd，再删/截段
    if (m_fd >= 0) { ::close(m_fd); m_fd = -1; }

    while (!m_segs.empty() && m_segs.back().base_idx >= index) {
        ::unlink(m_segs.back().path.c_str());
        m_segs.pop_back();
    }

    if (!m_segs.empty() && m_segs.back().lastIdx() >= index) {
        SegInfo& seg = m_segs.back();
        const uint64_t keep = index - seg.base_idx;     // 保留 keep 条

        const int fd = ::open(seg.path.c_str(), O_RDWR);
        if (fd < 0) {
            M_SYLAR_LOG_ERROR(g_logger) << "[log] 截断时打开段失败: " << seg.path;
            return m_sylar::IOState::FAILED;
        }
        // 顺序扫描算出第 keep 条记录的起始偏移
        off_t off = static_cast<off_t>(kLogHeaderSize);
        bool ok = true;
        for (uint64_t k = 0; k < keep && ok; ++k) {
            uint32_t net_len = 0;
            if (!readExactAt(fd, &net_len, sizeof(net_len), off)) { ok = false; break; }
            const uint32_t len = getU32(reinterpret_cast<const char*>(&net_len));
            if (len > kMaxRecordBytes) { ok = false; break; }
            off += static_cast<off_t>(sizeof(uint32_t)) + static_cast<off_t>(len);
        }
        char cnt[8] = {0};
        putU64(cnt, keep);
        if (!ok || ::ftruncate(fd, off) != 0 ||
            !pwriteAll(fd, cnt, sizeof(cnt), static_cast<off_t>(kLogEntryCountOffset)) ||
            ::fdatasync(fd) < 0) {
            ::close(fd);
            M_SYLAR_LOG_ERROR(g_logger) << "[log] 截断段失败: " << seg.path;
            return m_sylar::IOState::FAILED;
        }
        ::close(fd);
        seg.entry_count = keep;
        if (keep == 0) {
            ::unlink(seg.path.c_str());
            m_segs.pop_back();
        }
    }

    // 3) 调整位点
    m_last_log_idx = index - 1;
    uint64_t term = 0;
    m_last_log_term = (m_last_log_idx == 0) ? 0
                     : (lookupTerm(m_last_log_idx, term) ? term : 0);
    if (m_persist_idx > m_last_log_idx) { m_persist_idx = m_last_log_idx; }
    if (m_log_entries.empty()) {
        m_first_idx = m_last_log_idx + 1;
    } else if (m_first_idx > m_last_log_idx) {
        m_first_idx = m_last_log_idx + 1;
    }

    // 4) 重新打开写入段（日志还没空时）
    if (m_last_log_idx > 0) {
        if (ensureWriteFd() != m_sylar::IOState::SUCCESS) {
            return m_sylar::IOState::FAILED;
        }
    }
    return m_sylar::IOState::SUCCESS;
}

// ------------------------------------------------------------------ NodeLog：内存窗口装载/释放
m_sylar::Task<m_sylar::IOState> NodeLog::coLoadLogSeg(uint64_t to_idx) {
    if (to_idx < m_first_idx) {
        const SegInfo* seg = findSeg(to_idx);
        if (seg == nullptr) {
            M_SYLAR_LOG_ERROR(g_logger) << "[log] coLoadLogSeg: 索引 " << to_idx << " 找不到所属段";
            co_return m_sylar::IOState::FAILED;
        }
        if (seg->lastIdx() + 1 != m_first_idx) {
            M_SYLAR_LOG_ERROR(g_logger) << "[log] coLoadLogSeg: 段不连续，拒绝加载 " << seg->path;
            co_return m_sylar::IOState::FAILED;
        }
        std::vector<LogEntry> entries;
        uint64_t valid = 0;
        if (!readSegEntries(*seg, entries, valid) || valid != seg->entry_count) {
            co_return m_sylar::IOState::FAILED;
        }
        for (auto it = entries.rbegin(); it != entries.rend(); ++it) {
            m_log_entries.push_front(*it);              // 保持顺序：倒序 push_front
        }
        m_first_idx = seg->base_idx;
    }
    co_return m_sylar::IOState::SUCCESS;
}

m_sylar::Task<bool> NodeLog::coFreeLogSeg(uint64_t to_idx) {
    if (to_idx < m_first_idx) { co_return true; }       // 本来就不在内存里
    if (to_idx >= m_last_log_idx) { co_return false; }  // 至少保留最后一条（last_term 要用）
    uint64_t drop = to_idx - m_first_idx + 1;
    if (drop > m_log_entries.size()) { drop = m_log_entries.size(); }
    for (uint64_t k = 0; k < drop; ++k) {
        m_log_entries.pop_front();
    }
    m_first_idx = to_idx + 1;
    if (m_log_entries.empty()) { m_first_idx = m_last_log_idx + 1; }
    co_return true;
}

}   // namespace craft
