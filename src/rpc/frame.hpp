#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include <sylar/basic/config.h>

#include "rpcConfig.hpp"

/**
 * @brief  帧解析与构建
 */
namespace craft {
namespace RPC {

// repair: kLengthFieldSize / kMaxPayloadSize 原来就是这里的两个
//   `inline const size_t x = ConfigManager::LookUp(...)->getValue();` —— 那是纯静态快照：
//   LookUp 返回的临时 shared_ptr 在语句结束时当场析构、监听器被摘掉，值就冻结在
//   static 初始化那一刻（= 代码里的默认值），配置文件根本管不到它。
//   现在这两项和别的可调项一起挪进 rpcConfig.hpp，从 conf/rpc.json 读、走访问函数取：
//   用 kLengthFieldSize() / kMaxPayloadSize()。名字从"常量"变成"函数"是故意的 ——
//   让"这已经是运行期配置、不是编译期常量"这件事在调用点上看得到。
//   ★ kLengthFieldSize 是线上协议的一部分，改了必须两端一起改。

/**
 * @brief RPC 调用协议帧
 *        线上格式：| 4 字节大端长度头 | 载荷 |
 *     1) 永远用 size() / 带长度的构造与 append 判定长度，不要用 c_str() 当 C 串
 *     2) 不要用 << / >> 流操作符读写载荷（>> 遇空白即停，会截断）
 *     3) 追加字节用 append(ptr, len)，不要用 += std::string(ptr)
 */
class Frame {
public:
    using ptr = std::shared_ptr<Frame>;

    Frame() = default;

    /// 从原始内存构造载荷
    Frame(const char* data, size_t size);

    /// 接管一份载荷（不含长度头）
    explicit Frame(std::string payload);

    /// 从 payload 的 [0, length) 截取构造
    Frame(const std::string& payload, size_t length);

    Frame(const Frame& other) = default;
    Frame(Frame&& other) noexcept = default;
    Frame& operator=(const Frame& other) = default;
    Frame& operator=(Frame&& other) noexcept = default;

    ~Frame() = default;

    size_t size() const { return m_payload.size(); }
    bool empty() const { return m_payload.empty(); }

    /// 载荷（不含长度头）
    const std::string& payload() const { return m_payload; }
    /// 可直接作为 protobuf 的 ParseFromString / SerializeToString 目标
    std::string& payload() { return m_payload; }

    /// 线上字节：4 字节大端长度头 + 载荷
    std::string toWire() const;

    /// 设置载荷（从原始内存，取前 size 字节）
    void setData(const char* buffer, size_t size);
    /// 设置载荷（从 string，取前 size 字节）
    void setData(const std::string& buffer);

private:
    std::string m_payload;
};

}  // namespace RPC
}  // namespace craft
