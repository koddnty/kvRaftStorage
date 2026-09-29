//
// rpc 配置反序列化测试
//
// 分两块：
//   A. 纯函数：直接喂 json 给 FormatConversion<nlohmann::json, RpcDefine>，覆盖各种畸形配置。
//      转换器是本模块最容易写出"裸取不存在的 key"这种崩溃的地方（config.h:62 专门提醒过），
//      所以坏输入要一条条试。
//   B. 集成：走真实的 ConfigManager::LoadJson + LookUp，读仓库里那份 conf/rpc.json，
//      然后真的建一个 RPCClient。这一块是为了盯住"ConfigVar 构造即快照"那个坑 ——
//      顺序一旦写错（比如把 ConfigVar 写成全局 static），配置会被静默忽略、peer 数变 0，
//      下面 B1 立刻就会红。
//
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/syscall.h>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <string>
#include <unistd.h>

#include <nlohmann/json.hpp>
#include <sylar/basic/address.h>
#include <sylar/coroutine/corobase.h>

#include "rpc/payload.pb.h"
#include "rpc/rpcClient.hpp"
#include "rpc/rpcConfig.hpp"

using namespace craft::RPC;

static int g_pass = 0;
static int g_fail = 0;

// 探一个确定没人监听的空闲端口。
// repair: B6 原来直接拿 conf/rpc.json 里的 8808/8810 去测"对端没起"，但只要你本地
//   开着 ./kvRaft -i 0 / -i 2，那两个端口就是有人在听的 —— 客户端会真的连上、真的拿到回包
//   （未注册的方法会回 UNK_METHOD，而 RPCSession::call 不看 code），断言就变成 SUCCESS。
//   探一个空闲端口来测，测试才不依赖"本机有没有跑着集群"。
static uint16_t probeFreePort() {
    const int lfd = static_cast<int>(::syscall(SYS_socket, AF_INET, SOCK_STREAM, 0));
    int on = 1;
    ::syscall(SYS_setsockopt, lfd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

    sockaddr_in sa{};
    sa.sin_family = AF_INET;
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    sa.sin_port = 0;
    if (::syscall(SYS_bind, lfd, reinterpret_cast<sockaddr*>(&sa), sizeof(sa)) != 0) return 0;
    socklen_t len = sizeof(sa);
    ::syscall(SYS_getsockname, lfd, reinterpret_cast<sockaddr*>(&sa), &len);
    const uint16_t port = ntohs(sa.sin_port);
    ::syscall(SYS_close, lfd);
    return port;
}

static void check(bool ok, const char* name, const std::string& detail = "") {
    if (ok) {
        std::printf("  \xe2\x9c\x93 %s\n", name);
        ++g_pass;
    } else {
        std::printf("  \xe2\x9c\x97 %s   %s\n", name, detail.c_str());
        ++g_fail;
    }
}

static RpcDefine parse(const std::string& text) {
    return m_sylar::FormatConversion<nlohmann::json, RpcDefine>()(nlohmann::json::parse(text));
}

static std::string dump(const RpcDefine& d) {
    std::string s = "errmsg={" + d.errmsg + "} nodes=";
    for (const auto& n : d.nodes) {
        s += "[" + std::to_string(n.id) + " " + n.addr() + "]";
    }
    return s;
}

// ============================== A. 纯转换 ==============================

static void testConversion() {
    // A1. 整个文档就是数组（conf/rpc.json 现在的形状）
    {
        const RpcDefine d = parse(R"([{"id":1,"ip":"127.0.0.1","port":8808},
                                      {"id":2,"ip":"127.0.0.1","port":8809}])");
        check(d.isValid() && d.nodes.size() == 2 && d.nodes[0].id == 1 && d.nodes[0].port == 8808 &&
                  d.nodes[1].port == 8809,
              "A1. 顶层是数组", dump(d));
    }

    // A2. 文档里带一层 peers
    {
        const RpcDefine d = parse(R"({"peers":[{"id":1,"ip":"10.0.0.1","port":100}]})");
        check(d.isValid() && d.nodes.size() == 1 && d.nodes[0].ip == "10.0.0.1" && d.nodes[0].port == 100,
              "A2. 对象里的 peers", dump(d));
    }

    // A3. nodes 作为 peers 的别名
    {
        const RpcDefine d = parse(R"({"nodes":[{"id":7,"ip":"1.2.3.4","port":9}]})");
        check(d.isValid() && d.nodes.size() == 1 && d.nodes[0].id == 7, "A3. nodes 别名", dump(d));
    }

    // A4. 找不到列表字段 -> 报错，不能崩
    check(!parse(R"({"foo":1})").isValid(), "A4. 缺节点列表字段 -> isValid()==false");
    // A5. 空数组
    check(!parse("[]").isValid(), "A5. 空数组 -> isValid()==false");
    // A6. 列表不是数组
    check(!parse(R"({"peers":123})").isValid(), "A6. peers 不是数组 -> isValid()==false");
    // A7. 元素缺 port
    {
        const RpcDefine d = parse(R"([{"id":1,"ip":"127.0.0.1"}])");
        check(!d.isValid() && d.errmsg.find("port") != std::string::npos,
              "A7. 元素缺 port -> 报错", dump(d));
    }
    // A8. 元素缺 id
    check(!parse(R"([{"ip":"127.0.0.1","port":80}])").isValid(), "A8. 元素缺 id -> isValid()==false");
    // A9. id 类型不对
    check(!parse(R"([{"id":"abc","ip":"127.0.0.1","port":80}])").isValid(),
          "A9. id 不是整数 -> isValid()==false");
    // A10. port 为 0
    check(!parse(R"([{"id":1,"ip":"127.0.0.1","port":0}])").isValid(),
          "A10. port 为 0 -> isValid()==false");
    // A11. id 重复
    {
        const RpcDefine d = parse(R"([{"id":1,"ip":"127.0.0.1","port":80},
                                      {"id":1,"ip":"127.0.0.1","port":81}])");
        check(!d.isValid() && d.errmsg.find("id 重复") != std::string::npos,
              "A11. id 重复 -> 报错", dump(d));
    }
    // A12. ip:port 重复
    {
        const RpcDefine d = parse(R"([{"id":1,"ip":"127.0.0.1","port":80},
                                      {"id":2,"ip":"127.0.0.1","port":80}])");
        check(!d.isValid() && d.errmsg.find("地址重复") != std::string::npos,
              "A12. 地址重复 -> 报错", dump(d));
    }
    // A13. 不写 ip -> 用默认回环，仍然有效
    {
        const RpcDefine d = parse(R"([{"id":1,"port":80}])");
        check(d.isValid() && d.nodes[0].ip == "127.0.0.1", "A13. 省略 ip -> 默认 127.0.0.1", dump(d));
    }
    // A14. 元素不是对象
    check(!parse(R"([1,2,3])").isValid(), "A14. 元素不是对象 -> isValid()==false");
}

// ============================== C. 配置项生效性 ==============================

// ★ 反面教材，故意留在这里当反例，别照抄：
//   namespace 作用域的 static 快照 —— 构造发生在 main 【之前】，那时 LoadJson 还没跑，
//   所以它永远是下面这个默认值；而且 LookUp(...) 返回的临时 shared_ptr 语句一结束就析构、
//   监听器一起被摘掉，快照就此冻结。配置文件里写什么都改不了它。
//   C2 会把这一行和正确写法 kRpcTimeoutUs() 摆在一起对比。
static const int64_t g_snapshotDefault = 424242;
static const int64_t g_snapshotBeforeLoadJson =
    m_sylar::ConfigManager::LookUp<int64_t>("kRpcTimeoutUs", g_snapshotDefault, kRpcConfId)->getValue();

static void testSettings(const std::string& conf) {
    // C1. LoadJson 之后，各访问函数拿到的都是 conf/rpc.json 里的值
    RpcDefine def;
    const int rt = loadRpcConfig(conf, kRpcConfId, def);
    check(rt == 0 && kRpcTimeoutUs() == 100000 && kTimeoutToDisconnectCount() == 3 &&
              kReconnectMaxAttempts() == 3 && kReconnectBackoffMs() == 5 &&
              kReconnectMaxBackoffMs() == 20 && kLengthFieldSize() == 4 &&
              kMaxPayloadSize() == 67108864,
          "C1. 各配置项都取到 conf/rpc.json 里的值",
          "timeout=" + std::to_string(kRpcTimeoutUs()) + " maxPayload=" +
              std::to_string(kMaxPayloadSize()));

    // C2. ★ 反面教材 vs 正确写法：同一个 key、同一个 config_id
    check(g_snapshotBeforeLoadJson == g_snapshotDefault && kRpcTimeoutUs() == 100000,
          "C2. \xe2\x98\x85 static 快照永远是默认值，访问函数才是文件值",
          "snapshot=" + std::to_string(g_snapshotBeforeLoadJson) + " (期望 424242) accessor=" +
              std::to_string(kRpcTimeoutUs()) + " (期望 100000)");

    // C3. 直接换掉 ConfigData 里的 json -> 监听器把新值刷进 ConfigVar。
    //     这就是"改配置文件重新 LoadJson 就生效"的底层机制，不用重启进程。
    m_sylar::ConfigManager::getConfigData(kRpcConfId)->setConfig(nlohmann::json::parse(R"({
        "peers": [{"id":0,"ip":"127.0.0.1","port":8808}],
        "kRpcTimeoutUs": 123456,
        "kTimeoutToDisconnectCount": 9,
        "kReconnectMaxAttempts": 7,
        "kReconnectBackoffMs": 11,
        "kReconnectMaxBackoffMs": 33,
        "kLengthFieldSize": 4,
        "kMaxPayloadSize": 1234
    })"));
    check(kRpcTimeoutUs() == 123456 && kTimeoutToDisconnectCount() == 9 &&
              kReconnectMaxAttempts() == 7 && kReconnectBackoffMs() == 11 &&
              kReconnectMaxBackoffMs() == 33 && kMaxPayloadSize() == 1234,
          "C3. 值改了之后访问函数跟着变（监听器生效 = 热更新可用）",
          "timeout=" + std::to_string(kRpcTimeoutUs()) + " attempts=" +
              std::to_string(kReconnectMaxAttempts()) + " maxPayload=" +
              std::to_string(kMaxPayloadSize()));

    // C4. 再 LoadJson 一次真文件 -> 回到文件里的值（顺手把状态复原，别影响后面的 B 组）
    RpcDefine back;
    loadRpcConfig(conf, kRpcConfId, back);
    check(kRpcTimeoutUs() == 100000 && kMaxPayloadSize() == 67108864 && kReconnectMaxAttempts() == 3,
          "C4. 重新 LoadJson 后恢复文件值", "timeout=" + std::to_string(kRpcTimeoutUs()));
}

// ============================== B. 集成 ==============================

static RPCClient::ptr g_client;
static bool g_done = false;

static m_sylar::Task<void, m_sylar::TaskBeginExecuter> runIntegration() {
    const std::string conf = std::string(KVRAFT_SRC_DIR) + "/conf/rpc.json";

    // B1. 真实文件 -> RpcDefine（走 ConfigManager::LoadJson + LookUp）
    RpcDefine def;
    const int lrt = loadRpcConfig(conf, kRpcConfId, def);
    check(lrt == 0 && def.isValid() && def.nodes.size() == 3,
          "B1. 读仓库里那份 conf/rpc.json 拿到 3 个节点",
          "rt=" + std::to_string(lrt) + " " + dump(def));

    // B2. 必须是文件里的值，不是默认值 —— 这条专门盯"ConfigVar 构造即快照"那个坑：
    //     顺序写错的话 def 会是空的（走 default_value），端口也就无从谈起
    check(def.nodes.size() == 3 && def.nodes[0].port == 8808 && def.nodes[1].port == 8809 &&
              def.nodes[2].port == 8810,
          "B2. 端口来自文件而不是默认值", dump(def));

    // B3. 读一个不存在的文件 -> 报错而不是崩
    {
        RpcDefine bad;
        const int rt2 = loadRpcConfig(std::string(KVRAFT_SRC_DIR) + "/conf/no_such_file.json",
                                      kRpcConfId, bad);
        check(rt2 != 0 && !bad.isValid() && !bad.errmsg.empty(), "B3. 文件不存在 -> 返回 -1 且有 errmsg",
              "rt=" + std::to_string(rt2) + " " + dump(bad));
    }

    // B4. RPCClient::init：3 个节点、我是 id=1 -> 应该只建 2 条连接
    // repair: init 的签名已经从 init(confPath, configId, selfId) 改成
    //   init(const RpcDefine&, selfId) —— 配置的读取统一由 loadRpcConfig 负责（见 B1）。
    g_client = std::make_shared<RPCClient>();
    const int peers = co_await g_client->init(def, /*selfId=*/1);
    check(peers == 2, "B4. init(selfId=1) 建了 2 个 peer（3 个节点里把自己摘掉）",
          "peers=" + std::to_string(peers));

    // B5. selfId 不在配置里 -> 报错
    {
        auto c2 = std::make_shared<RPCClient>();
        const int rt3 = co_await c2->init(def, /*selfId=*/99);
        check(rt3 == -1, "B5. selfId 不在配置里 -> init 返回 -1", "rt=" + std::to_string(rt3));
    }

    // B6. 对端不可达 -> call 必须快速失败返回 RETRY，不能 hang
    //     （同时验证"配置装载 -> 建连失败 -> 快速失败"整条链路是通的）
    // repair: 不再用 conf/rpc.json 里的 8808 来测"对端没起"（你本地跑着集群时它会误报），
    //   改成自己组一份指向【空闲端口】的配置。
    {
        RpcDefine dead;
        dead.nodes = {
            NodeDefine{0, "127.0.0.1", probeFreePort()},   // 自己，init 会摘掉
            NodeDefine{1, "127.0.0.1", probeFreePort()},   // 对端：确定没人监听
        };
        auto c = std::make_shared<RPCClient>();
        const int n = co_await c->init(dead, /*selfId=*/0);
        auto r = std::make_shared<std::string>("x");
        auto p = std::make_shared<std::string>();
        const CallState st = co_await c->call(0, "RaftRpc", "AppendEntries", r, p);
        check(n == 1 && st == CallState::RETRY,
              "B6. 对端不可达 -> call 返回 RETRY（快速失败）",
              "peers=" + std::to_string(n) + " state=" + std::to_string(static_cast<int>(st)));
    }

    g_done = true;
    co_return;
}

int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);
    setvbuf(stderr, nullptr, _IONBF, 0);

    std::printf("A. 转换器\n");
    testConversion();

    const std::string conf = std::string(KVRAFT_SRC_DIR) + "/conf/rpc.json";

    std::printf("\nC. 配置项生效性\n");
    testSettings(conf);

    std::printf("\nB. 集成（真实配置文件 + RPCClient）\n");
    auto* iom = new m_sylar::IOManager("rpc_config_test", 1);
    iom->schedule(m_sylar::TaskCoro20::create_coro(runIntegration));
    for (int i = 0; i < 200; ++i) {
        if (g_done) break;
        ::usleep(50 * 1000);
    }
    if (!g_done) {
        std::printf("  \xe2\x9c\x97 B0. 集成用例没跑完（超时 10s）\n");
        ++g_fail;
    }

    std::printf("\n结果: pass=%d fail=%d\n", g_pass, g_fail);

    // 和别的测试一样刻意跳过正常析构：IOManager::getInstance() 在非 IOManager 线程上返回
    // nullptr，而 ~Socket() 会调它的 closeFd() -> 空指针
    std::_Exit(g_fail == 0 ? 0 : 1);
}
