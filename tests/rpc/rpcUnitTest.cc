//
// RPC 帧协议单元测试：Frame 编解码 + Parser 流式重组
//
#include "rpc/frame.hpp"
#include "rpc/parser.hpp"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <cstdint>
#include <random>
#include <string>
#include <vector>

using namespace craft;

namespace {

// 构造线上帧：4 字节大端长度头 + 载荷
std::string makeWire(const std::string& payload) {
    return RPC::Frame(payload).toWire();
}

// 按大端读出长度头
uint32_t readLenBE(const std::string& wire) {
    const auto* p = reinterpret_cast<const unsigned char*>(wire.data());
    return (uint32_t(p[0]) << 24) | (uint32_t(p[1]) << 16) | (uint32_t(p[2]) << 8) | uint32_t(p[3]);
}

// 按 chunks 逐段喂入，返回期间取出的所有帧
std::vector<RPC::Frame::ptr> feedInChunks(RPC::Parser& parser, const std::string& data,
                                          std::vector<size_t> chunks) {
    std::vector<RPC::Frame::ptr> out;
    size_t pos = 0;
    for (size_t n : chunks) {
        if (pos >= data.size()) break;
        n = std::min(n, data.size() - pos);
        parser.parse(data.data() + pos, n);
        pos += n;
        while (auto f = parser.getFrame()) out.push_back(f);
    }
    return out;
}

}  // namespace

// ---------------------------------------------------------------------------
// Frame
// ---------------------------------------------------------------------------

TEST_CASE("Frame 构造与载荷访问", "[rpc][frame]") {
    // 载荷含 '\0'，验证 string 承载二进制数据不截断
    const std::string payload = std::string("hello\0world", 11);

    SECTION("从 string 构造") {
        RPC::Frame f(payload);
        CHECK(f.size() == payload.size());
        CHECK(f.payload() == payload);
        CHECK_FALSE(f.empty());
    }

    SECTION("从原始内存构造") {
        RPC::Frame f(payload.data(), payload.size());
        CHECK(f.payload() == payload);
    }

    SECTION("截取构造：只取前 n 字节") {
        RPC::Frame f(payload, 5);
        CHECK(f.size() == 5);
        CHECK(f.payload() == "hello");
    }

    SECTION("截取长度超过原串时取全部") {
        RPC::Frame f(payload, 999);
        CHECK(f.payload() == payload);
    }

    SECTION("空帧") {
        RPC::Frame f{std::string()};
        CHECK(f.empty());
        CHECK(f.size() == 0);
    }
}

TEST_CASE("Frame 线上格式", "[rpc][frame]") {
    SECTION("长度头为 4 字节大端，等于载荷长度") {
        const std::string payload = "abc";
        const std::string wire = makeWire(payload);

        CHECK(wire.size() == 4 + payload.size());
        CHECK(readLenBE(wire) == payload.size());
        CHECK(wire.compare(4, std::string::npos, payload) == 0);
    }

    SECTION("长度超过 255 时高位字节正确") {
        const std::string wire = makeWire(std::string(300, 'x'));

        CHECK(readLenBE(wire) == 300);
        CHECK(static_cast<unsigned char>(wire[0]) == 0);
        CHECK(static_cast<unsigned char>(wire[2]) == 1);   // 300 = 0x0000012C
        CHECK(static_cast<unsigned char>(wire[3]) == 0x2C);
    }

    SECTION("空载荷也有长度头") {
        const std::string wire = makeWire(std::string());
        CHECK(wire.size() == 4);
        CHECK(readLenBE(wire) == 0);
    }
}

TEST_CASE("Frame setData 与拷贝语义", "[rpc][frame]") {
    SECTION("setData 从原始内存取前 size 字节") {
        RPC::Frame f;
        const char* buf = "abcdef";
        f.setData(buf, 3);
        CHECK(f.payload() == "abc");
    }

    SECTION("setData 从 string 取前 size 字节") {
        RPC::Frame f;
        f.setData(std::string("abcdef"), 3);
        CHECK(f.payload() == "abc");
    }

    SECTION("setData 传空指针则清空") {
        RPC::Frame f(std::string("abc"));
        f.setData(nullptr, 0);
        CHECK(f.empty());
    }

    SECTION("拷贝构造与原对象互不影响") {
        RPC::Frame a(std::string("abc"));
        RPC::Frame b = a;
        b.payload() = "xyz";
        CHECK(a.payload() == "abc");
        CHECK(b.payload() == "xyz");
    }

    SECTION("赋值运算符可链式使用") {
        RPC::Frame a, b, c;
        c = b = a = RPC::Frame(std::string("abc"));
        CHECK(a.payload() == "abc");
        CHECK(b.payload() == "abc");
        CHECK(c.payload() == "abc");
    }

    SECTION("移动后源对象为空") {
        RPC::Frame a(std::string("abc"));
        RPC::Frame b = std::move(a);
        CHECK(b.payload() == "abc");
        CHECK(a.empty());
    }
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

TEST_CASE("Parser 完整帧往返", "[rpc][parser]") {
    // 载荷含 '\0'，验证往返二进制安全
    const std::string payload = std::string("AB\0CD", 5) + std::string(3, '\0') + "EF";
    const std::string wire = makeWire(payload);

    RPC::Parser parser;
    const size_t used = parser.parse(wire);

    CHECK(used == wire.size());

    auto frame = parser.getFrame();
    REQUIRE(frame != nullptr);
    CHECK(frame->payload() == payload);
    CHECK(parser.getFrame() == nullptr);   // 只产出一帧
}

TEST_CASE("Parser 半包重组", "[rpc][parser]") {
    const std::string payload = std::string("half\0packet", 11) + std::string(64, 'z');
    const std::string wire = makeWire(payload);

    SECTION("逐字节喂入") {
        std::vector<size_t> chunks(wire.size(), 1);
        RPC::Parser parser;
        auto frames = feedInChunks(parser, wire, chunks);

        REQUIRE(frames.size() == 1);
        CHECK(frames[0]->payload() == payload);
    }

    SECTION("长度头只到了 2 字节时不产出帧") {
        RPC::Parser parser;
        CHECK(parser.parse(wire.data(), 2) == 2);
        CHECK(parser.getFrame() == nullptr);
        CHECK(parser.state() == RPC::Parser::State::HEADER);

        // 补齐剩下的
        parser.parse(wire.data() + 2, wire.size() - 2);
        auto frame = parser.getFrame();
        REQUIRE(frame != nullptr);
        CHECK(frame->payload() == payload);
    }

    SECTION("载荷分两段到达") {
        RPC::Parser parser;
        parser.parse(wire.data(), 20);                       // 长度头 + 部分载荷
        CHECK(parser.getFrame() == nullptr);
        CHECK(parser.state() == RPC::Parser::State::PAYLOAD);

        parser.parse(wire.data() + 20, wire.size() - 20);
        auto frame = parser.getFrame();
        REQUIRE(frame != nullptr);
        CHECK(frame->payload() == payload);
    }

    SECTION("随机切分（固定种子，可复现）") {
        std::mt19937 gen(20260922);
        std::uniform_int_distribution<size_t> dist(1, 37);

        RPC::Parser parser;
        std::vector<size_t> chunks;
        size_t total = 0;
        while (total < wire.size()) {
            size_t n = dist(gen);
            chunks.push_back(n);
            total += n;
        }

        auto frames = feedInChunks(parser, wire, chunks);
        REQUIRE(frames.size() == 1);
        CHECK(frames[0]->payload() == payload);
    }
}

TEST_CASE("Parser 粘包", "[rpc][parser]") {
    const std::string a = "first";
    const std::string b = std::string("sec\0ond", 7);
    const std::string c = "";

    SECTION("一次喂入 3 条帧") {
        RPC::Parser parser;
        const std::string all = makeWire(a) + makeWire(b) + makeWire(c);

        CHECK(parser.parse(all) == all.size());

        auto f1 = parser.getFrame();
        auto f2 = parser.getFrame();
        auto f3 = parser.getFrame();
        REQUIRE(f1 != nullptr);
        REQUIRE(f2 != nullptr);
        REQUIRE(f3 != nullptr);
        CHECK(f1->payload() == a);
        CHECK(f2->payload() == b);
        CHECK(f3->empty());
        CHECK(parser.getFrame() == nullptr);
    }

    SECTION("4 条帧但最后一条只到一半") {
        RPC::Parser parser;
        const std::string wire = makeWire(a);
        const std::string data = wire + wire + wire + wire.substr(0, 6);

        const size_t used = parser.parse(data);
        CHECK(used == 3 * wire.size() + 6);   // 半条不消费

        int count = 0;
        while (parser.getFrame()) ++count;
        CHECK(count == 3);
    }
}

TEST_CASE("Parser 非法帧", "[rpc][parser]") {
    SECTION("长度头超过上限进入 BADFRAME") {
        std::string bad(4, '\0');
        bad[0] = static_cast<char>(0xFF);
        bad[1] = static_cast<char>(0xFF);
        bad[2] = static_cast<char>(0xFF);
        bad[3] = static_cast<char>(0xFF);

        RPC::Parser parser;
        parser.parse(bad);

        CHECK(parser.hasError());
        CHECK(parser.state() == RPC::Parser::State::BADFRAME);
        CHECK(parser.getFrame() == nullptr);
    }

    SECTION("BADFRAME 后不再消费数据") {
        std::string bad(4, '\0');
        bad[0] = static_cast<char>(0xFF);

        RPC::Parser parser;
        parser.parse(bad);
        REQUIRE(parser.hasError());

        // 再喂合法数据也不消费
        CHECK(parser.parse(makeWire("abc")) == 0);
        CHECK(parser.getFrame() == nullptr);
    }

    SECTION("clearState 后可复用") {
        std::string bad(4, '\0');
        bad[0] = static_cast<char>(0xFF);

        RPC::Parser parser;
        parser.parse(bad);
        REQUIRE(parser.hasError());

        parser.clearState();
        CHECK_FALSE(parser.hasError());

        const std::string wire = makeWire("abc");
        CHECK(parser.parse(wire) == wire.size());
        auto frame = parser.getFrame();
        REQUIRE(frame != nullptr);
        CHECK(frame->payload() == "abc");
    }
}

TEST_CASE("Parser 边界输入", "[rpc][parser]") {
    SECTION("空输入") {
        RPC::Parser parser;
        CHECK(parser.parse(nullptr, 0) == 0);
        CHECK(parser.parse(std::string()) == 0);
        CHECK(parser.getFrame() == nullptr);
    }

    SECTION("大载荷 1MB") {
        const std::string payload(1024 * 1024, 'q');
        RPC::Parser parser;
        const std::string wire = makeWire(payload);

        CHECK(parser.parse(wire) == wire.size());
        auto frame = parser.getFrame();
        REQUIRE(frame != nullptr);
        CHECK(frame->size() == payload.size());
        CHECK(frame->payload() == payload);
    }
}
