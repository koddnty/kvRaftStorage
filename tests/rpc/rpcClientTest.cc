//
// RPCClient 重连语义测试：真实 RPCServer + 一个"永远不在线"的端口
//
// repair: 这个文件原来挂的是【裸 syscall 手写的、可以上下线的假服务端】。现在换成：
//   · peer 0 = 真正的 RPCServer（正常往返；另用 Drop handler 模拟"服务端把连接切断"）
//   · peer 1 = 一个探出来但【没有 listener】的端口（模拟对端一直不在线）
//   这样测试里不用再维护第二份线上协议实现。原来假服务端的"上下线"两条路径现在分别由
//   Drop handler（切断已有连接）和 dead port（压根连不上）覆盖，语义还更清楚。
//
// 盯的三个坑（都是"对端掉线"才暴露的）：
//   1. 对端掉线后 call() 必须【快速失败】返回 RETRY，绝不能卡住 ——
//      旧实现里重连协程持着 m_infos[id].mutex 在 `while(rt)` 里死循环，而 call() 的
//      CLOSED 分支要 co_await 拿同一把锁，于是对该节点的所有 call() 永久阻塞。
//      Raft 里心跳协程就是这么被卡死的，leader 直接被选掉。
//   2. coConnect() 的重连尝试必须【封顶】后放弃，不能无限重试。
//   3. 服务端把连接切断之后，客户端要能自动重连、调用恢复。
//
// 整个用例挂了 alarm() 看门狗：一旦卡死就直接退出并打标记 —— 卡死本身就是第 1 条的证据。
//
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>

#include <sylar/basic/address.h>
#include <sylar/basic/socket.h>
#include <sylar/coroutine/corobase.h>
#include <sylar/coroutine/coro20/hook.h>

#include "rpc/frame.hpp"
#include "rpc/payload.pb.h"
#include "rpc/rpcClient.hpp"
#include "rpc/rpcConfig.hpp"
#include "rpc/rpcServer.hpp"

using namespace craft::RPC;

// ============================== 服务端 handler ==============================
// 具名自由函数，不用 lambda —— m_sylar 里 lambda 协程有 stack-use-after-return 的坑

static m_sylar::Task<void> hEcho(std::shared_ptr<Request> req, RPCSession::ptr s) {
    Response resp;
    resp.set_id(req->id());                 // ★ id 必须原样回
    resp.set_code(static_cast<int>(RpcCode::OK));
    resp.set_data("echo:" + req->data());

    Frame f;
    f.setData(resp.SerializeAsString());
    co_await s->co_sendMessage(f);
    co_return;
}

// 服务端主动把连接切断这个场景【没法测】：Socket 既没有 shutdown 半关，直接调 Session::close()
// 又会在 handleClient 正阻塞在同一 fd 上 recv 的时候把 fd 抽走 —— 实测直接段错误。
// 所以"对端恢复"改成用另一个端口上的真服务端来测（见用例 3），语义一样且安全。

// ============================== 工具 ==============================

static RPCClient::ptr g_client;
static int g_pass = 0;
static int g_fail = 0;
static std::atomic<bool> g_done{false};

static void check(bool ok, const char* name, const std::string& detail = "") {
    if (ok) {
        std::printf("  \xe2\x9c\x93 %s\n", name);
        ++g_pass;
    } else {
        std::printf("  \xe2\x9c\x97 %s   %s\n", name, detail.c_str());
        ++g_fail;
    }
}

static uint64_t nowUs() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

static std::string stateName(CallState st) {
    switch (st) {
        case CallState::SUCCESS:   return "SUCCESS";
        case CallState::RETRY:     return "RETRY";
        case CallState::TIMEOUT:   return "TIMEOUT";
        case CallState::FAILED:    return "FAILED";
        case CallState::UNK_NODE:  return "UNK_NODE";
        case CallState::UNK_SERVICE: return "UNK_SERVICE";
        case CallState::UNK_METHOD:  return "UNK_METHOD";
        case CallState::UNK_REQBYTES:return "UNK_REQBYTES";
    }
    return "?";
}

// 探一个空闲端口（探完就把 fd 关掉，端口空着）
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

static m_sylar::IPv4Address::ptr mkAddr(uint16_t port) {
    auto a = std::dynamic_pointer_cast<m_sylar::IPv4Address>(
        m_sylar::Address::LookupAnyIPAddress("127.0.0.1"));
    a->setPort(port);
    return a;
}

// 发一次调用并记录耗时
static m_sylar::Task<CallState> doCall(int node, const std::string& method,
                                       const std::string& body, std::string* resp, uint64_t* us) {
    auto r = std::make_shared<std::string>(body);
    auto p = std::make_shared<std::string>();

    const uint64_t t0 = nowUs();
    // repair: service / method 改成 const std::string& 了，直接传字面量/形参
    const CallState st = co_await g_client->call(node, "RaftRpc", method, r, p);
    if (us) *us = nowUs() - t0;
    if (resp) *resp = *p;
    co_return st;
}

// ============================== 用例 ==============================

m_sylar::Task<void, m_sylar::TaskBeginExecuter> runAll(uint16_t serverPort, uint16_t deadPort) {
    // ---------- 0. 起真服务端 ----------
    // ★ 必须在本协程里构造：TcpServer 的构造默认实参是 IOManager::getInstance()
    auto server = std::make_shared<RPCServer>();
    if (!server->bind(mkAddr(serverPort))) {
        check(false, "0. RPCServer bind + listen 成功", "port=" + std::to_string(serverPort));
        g_done.store(true);
        co_return;
    }
    server->registeRoute("RaftRpc", "AppendEntries", hEcho);
    server->start();

    // ---------- 1. 正常建连 + 往返 ----------
    // repair: RPCClient::call 现在开头加了 `if (m_state != State::READY) return FAILED`，
    //   而 m_state 只有在 init() 里才会变成 READY。所以这里改成走真实启动路径：
    //   组一份 RpcDefine（自己是 id 0，两个对端分别是真服务端和死端口），由 init() 去 addPeer。
    //   以前那样手调 addPeer 现在会让每次 call 直接 FAILED。
    //   注意 selfId 必须选【被摘掉的那个】，剩下的两个才是真对端 —— 一开始写成 selfId=2
    //   就把死端口当成了自己，两个 peer 都指向真服务端，2a/2b/2c 全过不了。
    RpcDefine def;
    def.nodes = {
        NodeDefine{0, "127.0.0.1", serverPort},     // 自己（selfId=0）：init 会摘掉，不会被连
        NodeDefine{1, "127.0.0.1", serverPort},     // peer 0 = 真服务端
        NodeDefine{2, "127.0.0.1", deadPort},       // peer 1 = 没有任何 listener
    };
    const int peers = co_await g_client->init(def, /*selfId=*/0);
    const int idUp = 0;
    const int idDown = 1;
    check(peers == 2, "1a. init(selfId=0) 建了 2 个 peer（3 个节点里摘掉自己）",
          "peers=" + std::to_string(peers));

    std::string resp;
    uint64_t us = 0;
    const int crt = co_await g_client->coConnect(idUp);
    check(crt == 0, "1b. coConnect 成功", "rt=" + std::to_string(crt));

    CallState st = co_await doCall(idUp, "AppendEntries", "hello", &resp, &us);
    check(st == CallState::SUCCESS && resp == "echo:hello", "1c. 调用成功（真 server echo）",
          "state=" + stateName(st) + " resp=" + resp + " us=" + std::to_string(us));

    // 已连接时 coConnect 应该直接返回，不该白跑一轮 connect
    const uint64_t t_re = nowUs();
    const int crt2 = co_await g_client->coConnect(idUp);
    const uint64_t re_us = nowUs() - t_re;
    check(crt2 == 0 && re_us < 20 * 1000, "1d. 已连接时 coConnect 立刻返回",
          "rt=" + std::to_string(crt2) + " us=" + std::to_string(re_us));

    // ---------- 2. 对端（死端口）不在线：coConnect 必须有限次尝试后放弃 ----------
    const uint64_t t_rec = nowUs();
    const int rrt = co_await g_client->coConnect(idDown);
    const uint64_t rec_us = nowUs() - t_rec;
    check(rrt != 0 && rec_us < 3 * 1000 * 1000,
          "2a. \xe2\x98\x85 对端不在线时 coConnect 有限次尝试后放弃（不再 while(rt) 死循环）",
          "rt=" + std::to_string(rrt) + " us=" + std::to_string(rec_us) + " (期望 rt!=0 且 <3s)");

    // ★ 核心回归用例。
    //   旧实现：call() 的 CLOSED/未连接分支要 co_await 拿 m_infos[id].mutex，而重连协程正持着
    //   那把锁在 `while(rt)` 里死循环 → 下面这三次调用会【永久阻塞】，看门狗会把它打出来。
    //   现在：state==false 时 call() 只读一个 atomic 就返回 RETRY，不碰任何锁。
    bool fast = true;
    std::string detail;
    for (int i = 0; i < 3; ++i) {
        uint64_t t1 = 0;
        const CallState s2 = co_await doCall(idDown, "AppendEntries", "dead-" + std::to_string(i), &resp, &t1);
        if (s2 != CallState::RETRY || t1 >= 30 * 1000) {
            fast = false;
            detail = "i=" + std::to_string(i) + " state=" + stateName(s2) +
                     " us=" + std::to_string(t1) + " (期望 RETRY 且 <30ms)";
            break;
        }
    }
    check(fast, "2b. \xe2\x98\x85 对端不在线时连续 3 次调用都立刻返回 RETRY（不再被重连协程的锁卡死）", detail);

    // 放弃重连之后依然要快速失败
    st = co_await doCall(idDown, "AppendEntries", "still-down", &resp, &us);
    check(st == CallState::RETRY, "2c. 放弃重连后，调用依然立刻返回 RETRY",
          "state=" + stateName(st) + " us=" + std::to_string(us));

    // ---------- 3. 对端恢复：在 peer 1 那个端口上起第二个真服务端 ----------
    //   这条路正是 Raft 的做法：连不上就先失败，靠周期性的 coConnectAll() 再试。
    //   （不去停/重启第一个 server —— TcpServer::stop() 只关监听 socket，
    //     已建立的连接不受影响，而且服务端 recv 超时是 30s，测起来太慢。）
    auto server2 = std::make_shared<RPCServer>();
    if (!server2->bind(mkAddr(deadPort))) {
        check(false, "3a. 对端恢复：第二个 server bind 成功", "port=" + std::to_string(deadPort));
        g_done.store(true);
        co_return;
    }
    server2->registeRoute("RaftRpc", "AppendEntries", hEcho);
    server2->start();
    co_await m_sylar::co_sleep(30);                 // 让 startAccept 先跑起来

    const uint64_t t_back = nowUs();
    const int rrt2 = co_await g_client->coConnect(idDown);
    check(rrt2 == 0 && nowUs() - t_back < 3 * 1000 * 1000,
          "3a. \xe2\x98\x85 对端恢复后 coConnect 成功（整体替换 RPCSession）",
          "rt=" + std::to_string(rrt2) + " us=" + std::to_string(nowUs() - t_back));

    st = co_await doCall(idDown, "AppendEntries", "back", &resp, &us);
    check(st == CallState::SUCCESS && resp == "echo:back", "3b. \xe2\x98\x85 重连后调用恢复正常",
          "state=" + stateName(st) + " resp=" + resp + " us=" + std::to_string(us));

    // 原来的 peer 0 全程没受影响（两个方向/两个节点互不牵连）
    st = co_await doCall(idUp, "AppendEntries", "up-still-ok", &resp, &us);
    check(st == CallState::SUCCESS && resp == "echo:up-still-ok",
          "3c. peer 1 的故障没有影响 peer 0（每条连接各自独立）",
          "state=" + stateName(st) + " resp=" + resp);

    g_done.store(true);
    co_return;
}

// ============================== main ==============================

static void onAlarm(int) {
    const char msg[] = "\n!! 看门狗超时：用例卡死了。\n"
                       "   这正是旧实现里 call() 被重连协程的锁卡住 / coConnect 死循环的症状。\n";
    ::write(STDERR_FILENO, msg, sizeof(msg) - 1);
    ::_exit(2);
}

int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);
    setvbuf(stderr, nullptr, _IONBF, 0);

    const uint16_t serverPort = probeFreePort();
    const uint16_t deadPort = probeFreePort();
    if (serverPort == 0 || deadPort == 0 || serverPort == deadPort) {
        std::printf("探测空闲端口失败\n");
        return 1;
    }
    std::printf("真 RPCServer 监听 127.0.0.1:%u；死端口 127.0.0.1:%u（无人监听）\n\n",
                serverPort, deadPort);

    ::signal(SIGALRM, onAlarm);
    ::alarm(20);

    g_client = std::make_shared<RPCClient>();

    auto* iom = new m_sylar::IOManager("rpc_client_test", 1);
    iom->schedule(m_sylar::TaskCoro20::create_coro(std::bind(runAll, serverPort, deadPort)));

    // 等 7 个用例跑完（最多 15s，超过就让看门狗兜底）
    for (int i = 0; i < 300; ++i) {
        if (g_done.load()) break;
        ::usleep(50 * 1000);
    }
    if (!g_done.load()) {
        std::printf("  \xe2\x9c\x97 0. 用例没跑完（超时 15s）\n");
        ++g_fail;
    }

    std::printf("\n结果: pass=%d fail=%d\n", g_pass, g_fail);

    // ★ 刻意跳过正常析构（理由同 rpcSessionTest）：
    //   IOManager::getInstance() 在非 IOManager 线程上返回 nullptr，而 ~Socket() 会调它的
    //   closeFd() → 空指针。
    ::alarm(0);
    std::_Exit(g_fail == 0 ? 0 : 1);
}
