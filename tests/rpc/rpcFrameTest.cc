//
// Created by koddnty on 2026/9/22.
//
#include <cassert>
#include <iostream>
#include <string>
#include "rpc/frame.hpp"
#include "rpc/parser.hpp"

using namespace craft;

int main() {
    // 1) 构造一帧（载荷里故意带 '\0'，验证 string 承载二进制数据不截断）
    const std::string original = std::string("我喜欢你啊") + std::string(1, '\0') + "tail";
    RPC::Frame frame(original);

    // 2) 取【线上字节】：4 字节大端长度头 + 载荷
    const std::string wire = frame.toWire();

    // 3) 解析
    RPC::Parser parser;
    const size_t used = parser.parse(wire);
    assert(used == wire.size());

    RPC::Frame::ptr output = parser.getFrame();
    if (output == nullptr) {
        std::cerr << "解析失败：没有取到完整帧\n";
        return 1;
    }

    // 4) 往返一致
    std::cout << "原始长度: " << original.size() << "\n";
    std::cout << "解析长度: " << output->size() << "\n";
    std::cout << "载荷内容: " << output->payload() << "\n";
    assert(output->payload() == original);

    std::cout << "round-trip OK" << std::endl;
    return 0;
}
