//
// ArgParser 单元测试：匹配 / 取值 / 回调 / 错误路径
//
#include "tools/argParser.hpp"

#include <catch2/catch_test_macros.hpp>

#include <cstdlib>
#include <string>
#include <vector>

using namespace craft;

namespace {

// 一套典型的注册：-i/--node-id 和 -c/--conf，和 main.cc 里的一样
struct Fixture {
    int nodeId{-1};
    std::string conf{"default.json"};
    int idCalls{0};
    int confCalls{0};
    ArgParser parser{"kvRaft"};

    Fixture() {
        parser.addOption("-i", "--node-id", "本节点 id", [this](const std::string& v) {
            ++idCalls;
            nodeId = std::atoi(v.c_str());
            return nodeId >= 0;
        });
        parser.addOption("-c", "--conf", "配置文件路径", [this](const std::string& v) {
            ++confCalls;
            conf = v;
            return !v.empty();
        });
    }
};

}  // namespace

TEST_CASE("ArgParser: 短名 / 长名 / 等号 三种写法都能解析", "[tools]") {
    SECTION("短名 + 空格") {
        Fixture f;
        REQUIRE(f.parser.parse({"-i", "2", "-c", "a.json"}) == ArgParser::Result::OK);
        REQUIRE(f.nodeId == 2);
        REQUIRE(f.conf == "a.json");
        REQUIRE(f.idCalls == 1);
        REQUIRE(f.confCalls == 1);
    }
    SECTION("长名 + 空格") {
        Fixture f;
        REQUIRE(f.parser.parse({"--node-id", "2", "--conf", "a.json"}) == ArgParser::Result::OK);
        REQUIRE(f.nodeId == 2);
        REQUIRE(f.conf == "a.json");
    }
    SECTION("长名 + 等号") {
        Fixture f;
        REQUIRE(f.parser.parse({"--node-id=2", "--conf=a.json"}) == ArgParser::Result::OK);
        REQUIRE(f.nodeId == 2);
        REQUIRE(f.conf == "a.json");
    }
    SECTION("顺序无关、混着写") {
        Fixture f;
        REQUIRE(f.parser.parse({"--conf=b.json", "-i", "0"}) == ArgParser::Result::OK);
        REQUIRE(f.nodeId == 0);
        REQUIRE(f.conf == "b.json");
    }
}

TEST_CASE("ArgParser: 没出现的选项不调回调（默认值保留）", "[tools]") {
    Fixture f;
    REQUIRE(f.parser.parse({}) == ArgParser::Result::OK);
    REQUIRE(f.nodeId == -1);            // 初值没被动过
    REQUIRE(f.conf == "default.json");
    REQUIRE(f.idCalls == 0);
    REQUIRE(f.confCalls == 0);
}

TEST_CASE("ArgParser: 错误路径都返回 Error", "[tools]") {
    SECTION("未知参数") {
        Fixture f;
        REQUIRE(f.parser.parse({"-x", "1"}) == ArgParser::Result::Error);
    }
    SECTION("缺少值") {
        Fixture f;
        REQUIRE(f.parser.parse({"-i"}) == ArgParser::Result::Error);
    }
    SECTION("回调返回 false") {
        Fixture f;
        REQUIRE(f.parser.parse({"-i", "-5"}) == ArgParser::Result::Error);   // nodeId < 0
        REQUIRE(f.idCalls == 1);
    }
    SECTION("空值也算非法") {
        Fixture f;
        REQUIRE(f.parser.parse({"--conf="}) == ArgParser::Result::Error);
    }
    SECTION("未知长名带等号 -> 当作未知参数") {
        Fixture f;
        REQUIRE(f.parser.parse({"--nope=1"}) == ArgParser::Result::Error);
    }
}

TEST_CASE("ArgParser: 值以 '-' 开头也照取（和 getopt 一致）", "[tools]") {
    // "-i -1" 不能被误判成「缺少值」；合不合法由回调判断
    Fixture f;
    REQUIRE(f.parser.parse({"-i", "-1"}) == ArgParser::Result::Error);   // 回调拒了
    REQUIRE(f.idCalls == 1);                                             // 但值确实传进来了
}

TEST_CASE("ArgParser: -h / --help 返回 Help 且不碰回调", "[tools]") {
    for (const auto& flag : {std::string("-h"), std::string("--help")}) {
        Fixture f;
        REQUIRE(f.parser.parse({flag}) == ArgParser::Result::Help);
        // help 出现在别的参数之后也要生效
        Fixture g;
        REQUIRE(g.parser.parse({"-i", "1", flag}) == ArgParser::Result::Help);
        REQUIRE(g.confCalls == 0);
    }
}

TEST_CASE("ArgParser: usage 列出注册的选项和说明", "[tools]") {
    Fixture f;
    const std::string u = f.parser.usage();
    REQUIRE(u.find("kvRaft") != std::string::npos);
    REQUIRE(u.find("-i") != std::string::npos);
    REQUIRE(u.find("--node-id") != std::string::npos);
    REQUIRE(u.find("-c") != std::string::npos);
    REQUIRE(u.find("--conf") != std::string::npos);
    REQUIRE(u.find("本节点 id") != std::string::npos);
    REQUIRE(u.find("--help") != std::string::npos);
}

TEST_CASE("ArgParser: 同一个选项出现两次，以最后一次为准", "[tools]") {
    Fixture f;
    REQUIRE(f.parser.parse({"-i", "1", "-i", "2"}) == ArgParser::Result::OK);
    REQUIRE(f.nodeId == 2);
    REQUIRE(f.idCalls == 2);
}
