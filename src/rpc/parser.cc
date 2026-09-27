#include "parser.hpp"

#include <algorithm>

namespace craft {
namespace RPC {

size_t Parser::parse(const std::string& buffer) {
    return parse(buffer.data(), buffer.size());
}

size_t Parser::parse(const char* buffer, size_t size) {
    if (buffer == nullptr || size == 0) {
        return 0;
    }

    size_t idx = 0;
    while (idx < size) {
        if (m_state == State::BADFRAME) {
            break;  // 已损坏，不再消费（调用方应关闭连接）
        }

        // ---- 1) 收长度头（4 字节）----
        if (m_length_buffer.size() < kLengthFieldSize()) {
            m_state = State::HEADER;
            const size_t need = kLengthFieldSize() - m_length_buffer.size();
            const size_t take = std::min(need, size - idx);
            m_length_buffer.append(buffer + idx, take);  // ★ append(ptr, len)
            idx += take;

            if (m_length_buffer.size() < kLengthFieldSize()) {
                break;  // 长度头还没凑齐，等下次
            }
            // ★ 必须【凑齐 4 字节之后】才解析，否则会解出垃圾长度
            if (!parseHeader()) {
                m_state = State::BADFRAME;
                break;
            }
        }

        // ---- 2) 收载荷 ----
        m_state = State::PAYLOAD;
        const size_t need = m_payload_length - m_payload_buffer.size();
        const size_t take = std::min(need, size - idx);
        m_payload_buffer.append(buffer + idx, take);
        idx += take;

        if (m_payload_buffer.size() < m_payload_length) {
            break;  // 载荷还没收满，等下次
        }

        // ---- 3) 一帧完成：入队、重置、继续处理后面的字节（粘包）----
        m_parsed_frames.push_back(std::make_shared<Frame>(std::move(m_payload_buffer)));
        m_payload_buffer.clear();
        m_payload_length = 0;
        m_length_buffer.clear();
        m_state = State::READY;
    }
    return idx;
}

Frame::ptr Parser::popFrame() {
    if (m_parsed_frames.empty()) {
        return nullptr;
    }
    auto rt = m_parsed_frames.front();
    m_parsed_frames.pop_front();
    return rt;
}

bool Parser::parseHeader() {
    if (m_length_buffer.size() != kLengthFieldSize()) {
        return false;
    }
    // 按【网络序/大端】还原长度。
    // 注意：这个循环本身就已经把大端字节解成主机值了，
    //       不要再调用 byteSwap* —— 那是「主机序 → 小端」，用在这里是错的
    //       （在本机小端上恰好是恒等操作，所以错了也不报错，但大端机上会直接损坏）
    const auto* p = reinterpret_cast<const unsigned char*>(m_length_buffer.data());
    const uint32_t len = (static_cast<uint32_t>(p[0]) << 24) | (static_cast<uint32_t>(p[1]) << 16) |
                         (static_cast<uint32_t>(p[2]) << 8) | static_cast<uint32_t>(p[3]);

    if (len > kMaxPayloadSize()) {
        m_payload_length = 0;
        return false;  // 触发 BADFRAME，避免恶意长度头撑爆内存
    }
    m_payload_length = len;
    return true;
}

void Parser::clearState() {
    m_payload_buffer.clear();
    m_payload_length = 0;
    m_length_buffer.clear();
    m_state = State::HEADER;
}

}  // namespace RPC
}  // namespace craft
