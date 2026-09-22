#pragma once
#include <memory>
#include <vector>

/**
 * @brief  帧解析与构建
 */
namespace craft {
namespace RPC {


/**
 * @brief RPC 调用协议帧传输
 *          4字节长度头 | 载荷
 *          暂时不分片
 */
class Frame : public std::enable_shared_from_this<Frame>{
public:
    Frame(const std::vector<uint8_t>& data, int length = -1);
    Frame(std::vector<uint8_t>&& data, int length = -1);
    Frame(const Frame& other);
    Frame(Frame&& other);
    void operator=(const Frame& other);
    void operator=(Frame&& other);

    /**
     * @brief  此函数会清空当前Frame,请谨慎使用
     */
    void setSize(size_t size);
    inline size_t getParsedSize() {return m_parsed; }

    /**
     *  @brief 调用parse将会解析数据，自动设置长度并设置载荷，注意这之间不要调用其他函数导致状态损坏
     *  @return 返回解析的数据长度
     */
    size_t parse(std::vector<uint8_t> buffer);
    size_t parse(const char* buffer, size_t size);      // size为buffer长度

    std::vector<uint8_t>& getData() {return m_buffer;}
    std::vector<uint8_t> copyData() {return m_buffer; }

    /**
     *  @brief 直接设置数据以及长度（与parse区别在于其需要完整报文并自己手动解析
     */
    void setData(char* buffer, size_t size);
    size_t parse(std::vector<uint8_t> buffer);
private:
    size_t m_size{0};
    size_t m_parsed{0};
    std::vector<uint8_t> m_buffer{};
};

}
}