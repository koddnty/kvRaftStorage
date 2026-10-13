#include "log.hpp"

#include <arpa/inet.h>
#include <array>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>
#include <system_error>
#include <vector>


namespace craft {



// ---------------------------------------------------------------- 大端字段编解码
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



// ---------------------------------------------------------------- 同步 IO 内核
static bool writeAllSync(int fd, const void* buffer, size_t size) {
    const auto* ptr = static_cast<const char*>(buffer);
    size_t written = 0;

    while (written < size) {
        const ssize_t n = ::write(fd, ptr + written, size - written);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (n == 0) {
            return false;
        }
        written += static_cast<size_t>(n);
    }
    return true;
}

// 失败时抛异常（和原来的 readAllAt 语义一致，便于 readLogFile 用 try/catch 兜）
static void readAllAtSync(int fd, void* buffer, size_t size, uint64_t offset) {
    auto* ptr = static_cast<char*>(buffer);
    size_t received = 0;

    while (received < size) {
        const ssize_t n = ::pread(
            fd,
            ptr + received,
            size - received,
            static_cast<off_t>(offset + received));

        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            throw std::system_error(errno, std::generic_category(), "pread log file");
        }
        if (n == 0) {
            throw std::runtime_error("Unexpected EOF in log file");
        }
        received += static_cast<size_t>(n);
    }
}

// 读分片文件头。false = 空文件 / 不足 32 字节 / magic 不对 / 版本不认识 / 读失败（meta 已清空）
static bool readHeaderSync(int fd, LogFileMeta& meta) {
    meta = LogFileMeta{};

    if (fd < 0) {
        return false;
    }

    std::array<char, kLogHeaderSize> header{};
    size_t received = 0;
    while (received < header.size()) {
        const ssize_t n = ::pread(fd, header.data() + received, header.size() - received,
                                  static_cast<off_t>(received));
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;       // 探测性读取：不抛，调用方要能安全地判定"这不是分片文件"
        }
        if (n == 0) {
            return false;       // 空文件，或头被截断
        }
        received += static_cast<size_t>(n);
    }

    if (getU32(header.data() + 0) != kLogHeaderMagic) {
        return false;           // 裸记录的老文件：调用方退回"偏移 0"解析
    }

    const uint16_t version = getU16(header.data() + 4);
    const uint16_t header_size = getU16(header.data() + 6);

    if (version == 0 || version > kLogHeaderVersion) {
        return false;           // 比当前实现更新的版本，不敢按现在的布局解
    }
    if (header_size < kLogHeaderSize) {
        return false;           // 头长非法，记录区起点会算错
    }

    meta.header_size = header_size;
    meta.base_idx = getU64(header.data() + 8);
    meta.entry_count = getU64(header.data() + 16);
    return true;
}

// 就地改写文件头里的 entry_count（只动 8 个字节，不 fsync）
static bool updateEntryCountSync(int fd, uint64_t entry_count) {
    if (fd < 0) {
        return false;
    }

    std::array<char, sizeof(uint64_t)> buf{};
    putU64(buf.data(), entry_count);

    size_t written = 0;
    while (written < buf.size()) {
        const ssize_t n = ::pwrite(fd, buf.data() + written, buf.size() - written,
                                   static_cast<off_t>(kLogEntryCountOffset + written));
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (n == 0) {
            return false;
        }
        written += static_cast<size_t>(n);
    }
    return true;
}

// 追加一条日志：记录写到文件末尾 → 文件头 entry_count +1 → 一次 fdatasync。
// 任一步失败都把文件截回追加前的位置，保证"文件里的条数"和头里的计数永远对得上。
static bool appendRecordSync(int fd, const LogEntry& entry) {
    if (fd < 0) {
        return false;
    }

    const std::string data = entry.SerializeAsString();

    if (data.size() > std::numeric_limits<uint32_t>::max()) {
        return false;
    }

    const uint32_t len = htonl(static_cast<uint32_t>(data.size()));

    const off_t start = ::lseek(fd, 0, SEEK_END);
    if (start < 0) {
        return false;
    }

    // 回滚点：把追加进去的字节截掉，并把文件偏移复位
    const auto rollback = [fd, start]() {
        const int saved_errno = errno;
        if (::ftruncate(fd, start) == 0) {
            ::lseek(fd, start, SEEK_SET);
        }
        errno = saved_errno;
    };

    if (!writeAllSync(fd, &len, sizeof(len)) ||
        !writeAllSync(fd, data.data(), data.size())) {
        rollback();
        return false;
    }

    // 计数 +1（裸记录老文件没有文件头，跳过，保持兼容）
    LogFileMeta meta{};
    if (readHeaderSync(fd, meta)) {
        if (!updateEntryCountSync(fd, meta.entry_count + 1)) {
            rollback();
            return false;
        }
    }

    if (::fdatasync(fd) < 0) {
        return false;
    }
    return true;
}



// ---------------------------------------------------------------- 分片文件头（对外 Task 接口）


m_sylar::Task<bool> LogEntryWrapper::create(int fd, uint64_t base_idx) {
    if (fd < 0) {
        co_return false;
    }

    std::array<char, kLogHeaderSize> header{};      // 全 0 起手，reserved 自然也是 0
    putU32(header.data() + 0, kLogHeaderMagic);
    putU16(header.data() + 4, kLogHeaderVersion);
    putU16(header.data() + 6, static_cast<uint16_t>(kLogHeaderSize));
    putU64(header.data() + 8, base_idx);
    putU64(header.data() + 16, 0);                  // 新分片里还没有日志

    if (::ftruncate(fd, 0) < 0) {                   // 重建语义：老内容（含旧头）全部丢掉
        co_return false;
    }

    if (::lseek(fd, 0, SEEK_SET) < 0) {             // 写依赖文件偏移
        co_return false;
    }

    if (!writeAllSync(fd, header.data(), header.size())) {
        co_return false;
    }

    if (::fdatasync(fd) < 0) {                      // 头是"这文件属于本模块"的凭据，落盘再返回
        co_return false;
    }

    co_return true;
}

m_sylar::Task<bool> LogEntryWrapper::readHeader(int fd, LogFileMeta& meta) {
    co_return readHeaderSync(fd, meta);
}

m_sylar::Task<bool> LogEntryWrapper::updateEntryCount(int fd, uint64_t entry_count) {
    co_return updateEntryCountSync(fd, entry_count);
}



m_sylar::Task<bool> LogEntryWrapper::persist() const {
    co_return appendRecordSync(m_fd, m_entry);
}

// 读取整个日志文件，返回按文件顺序排列的日志条目
// 使用 pread，不修改 fd 当前的文件偏移量
m_sylar::Task<std::vector<LogEntry>> LogEntryWrapper::readLogFile(int fd) {
    if (fd < 0) {
        throw std::invalid_argument("Invalid log file descriptor");
    }

    struct stat st{};
    if (::fstat(fd, &st) < 0) {
        throw std::system_error(
            errno, std::generic_category(), "fstat log file");
    }

    if (st.st_size < 0) {
        throw std::runtime_error("Invalid log file size");
    }

    const uint64_t file_size = static_cast<uint64_t>(st.st_size);


    LogFileMeta meta{};
    uint64_t offset = 0;
    if (readHeaderSync(fd, meta)) {
        offset = meta.header_size;
    }

    std::vector<LogEntry> entries;

    while (offset < file_size) {
        uint32_t net_len = 0;

        if (file_size - offset < sizeof(net_len)) {
            throw std::runtime_error("Incomplete log length header");
        }

        readAllAtSync(fd, &net_len, sizeof(net_len), offset);
        offset += sizeof(net_len);

        const uint32_t len = ntohl(net_len);

        if (len > kMaxRecordSize) {
            throw std::runtime_error("Log record too large");
        }

        if (len > file_size - offset) {
            throw std::runtime_error("Incomplete log record");
        }

        std::string data(len, '\0');
        readAllAtSync(fd, data.data(), data.size(), offset);
        offset += len;

        LogEntry entry;
        if (!entry.ParseFromString(data)) {
            throw std::runtime_error("Invalid protobuf log record");
        }

        entries.emplace_back(std::move(entry));
    }

    co_return entries;
}



m_sylar::Task<bool> LogEntryWrapper::writeAll(int fd, const void* buffer, size_t size) {
    co_return writeAllSync(fd, buffer, size);
}


m_sylar::Task<void> LogEntryWrapper::readAllAt(int fd, void* buffer, size_t size, uint64_t offset) {
    readAllAtSync(fd, buffer, size, offset);
    co_return;
}


// nodelog ----------------------------------------
void NodeLog::appendLog(const LogEntryWrapper& entry) {
    m_log_entries.push_back(entry);
    m_last_log_idx++;
    m_last_log_term = entry.getEntry().term();
}


m_sylar::IOState NodeLog::persistLog(uint64_t idx) {
    if (idx > m_last_log_idx) {
        return m_sylar::IOState::FAILED;            // 这条日志还没 append 进内存，落不了盘
    }
    if (idx <= m_persist_idx) {
        return m_sylar::IOState::SUCCESS;           // 幂等：已经持久化到这个位置了（idx==0 也走这里）
    }
    if (m_fd < 0) {
        return m_sylar::IOState::FAILED;            // 还没 attach 分片文件
    }

    for (uint64_t i = m_persist_idx + 1; i <= idx; ++i) {
        const uint64_t behind = m_last_log_idx - i;                         // 离最后一条有多远
        if (behind >= m_log_entries.size()) {
            return m_sylar::IOState::FAILED;        // 这条日志已经不在内存里了（被裁剪/m_last_log_idx 与 vector 不一致）
        }
        const size_t local = m_log_entries.size() - 1 - static_cast<size_t>(behind);


        if (!appendRecordSync(m_fd, m_log_entries[local].getEntry())) {
            return m_sylar::IOState::FAILED;        // 一条失败就停在这里：绝不跳过没落盘的日志去写后面的
        }
        m_persist_idx = i;
    }

    return m_sylar::IOState::SUCCESS;
}


bool NodeLog::commitLog(uint64_t idx) {
    if (idx > m_last_log_idx) {
        return false;                               // 没有这条日志
    }
    if (idx > m_persist_idx) {
        return false;                               // 没落盘的日志不许提交
    }
    if (idx <= m_commit_idx) {
        return true;                                // 已经提交过（含 idx == m_commit_idx 的重复调用）
    }

    // 提交位点只在内存里推进：Raft 的 commitIndex 本来就是易失的，
    // 重启后由 leader 通过 AppendEntries 的 leaderCommit 重新告知，不需要落盘。
    m_commit_idx = idx;
    return true;
}


}
