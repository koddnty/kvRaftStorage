#pragma once

#include <cstddef>
#include <cstdint>
#include <list>
#include <string>

#include "frame.hpp"

namespace craft {
namespace RPC {

/**
 * @brief RPC 帧解析器（流式）
 *
 * 每个连接持有一个实例。它维护「半包状态」：长度头收到一半、载荷收到一半，
 * 下次 feed 进来继续拼。TCP 是字节流，这一步绕不过去。
 *
 * 用法：
 *     Parser p;
 *     size_t used = p.parse(buf.data(), buf.size());   // 消费 used 字节
 *     while (auto f = p.getFrame()) { ... }            // 取出所有完整帧
 *     if (p.hasError()) { 关闭连接; }
 */
class Parser {
public:
    using ptr = std::shared_ptr<Parser>;
    /**
     * @brief 喂入数据并解析出尽可能多的完整帧（一次喂入多条也能全部产出）
     * @return 实际消费的字节数（调用方应从 buffer 头部丢弃这么多）
     */
    size_t parse(const std::string& buffer);
    size_t parse(const char* buffer, size_t size);

    /// 取出一帧并删除对应帧
    Frame::ptr popFrame();

    enum class State {
        READY = 0,    // 一帧刚就绪（瞬时状态）
        HEADER = 1,   // 正在收长度头
        PAYLOAD = 2,  // 正在收载荷
        BADFRAME = 3  // 帧损坏（长度非法），调用方应关闭连接
    };

    [[nodiscard]] inline bool empty() const {return m_parsed_frames.empty();}


    [[nodiscard]] State state() const { return m_state; }
    [[nodiscard]] bool hasError() const { return m_state == State::BADFRAME; }

    /// 清空所有中间状态（连接复用时调用）
    void clearState();

private:
    /// 长度头凑齐后解析；返回 false 表示长度非法
    bool parseHeader();

private:
    std::string m_payload_buffer;                    // 正在收的载荷
    uint32_t m_payload_length{0};                    // 本帧载荷应有长度
    std::string m_length_buffer;                     // 攒长度头（最多 4 字节）
    State m_state{State::HEADER};

    std::list<Frame::ptr> m_parsed_frames;           // 已就绪的帧
};

}  // namespace RPC
}  // namespace craft
