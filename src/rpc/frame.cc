#include "frame.hpp"

#include <algorithm>

namespace craft {
namespace RPC {

namespace {

/// 把 32 位长度按大端（网络序）追加到 out
inline void appendLengthBE(std::string* out, uint32_t n) {
    out->push_back(static_cast<char>((n >> 24) & 0xFF));
    out->push_back(static_cast<char>((n >> 16) & 0xFF));
    out->push_back(static_cast<char>((n >> 8) & 0xFF));
    out->push_back(static_cast<char>(n & 0xFF));
}

}  // namespace

Frame::Frame(const char* data, size_t size) {
    if (data != nullptr && size > 0) {
        m_payload.assign(data, size);  // ★ assign(ptr, len)，不是 += std::string(ptr)
    }
}

Frame::Frame(std::string payload) : m_payload(std::move(payload)) {}

Frame::Frame(const std::string& payload, size_t length)
    : m_payload(payload, 0, std::min(length, payload.size())) {}

void Frame::setData(const char* buffer, size_t size) {
    if (buffer == nullptr || size == 0) {
        m_payload.clear();
        return;
    }
    m_payload.assign(buffer, size);
}

void Frame::setData(const std::string& buffer) {
    m_payload.assign(buffer, 0, buffer.size());
}

std::string Frame::toWire() const {
    std::string out;
    out.reserve(kLengthFieldSize() + m_payload.size());
    appendLengthBE(&out, static_cast<uint32_t>(m_payload.size()));
    out.append(m_payload);
    return out;
}

}  // namespace RPC
}  // namespace craft
