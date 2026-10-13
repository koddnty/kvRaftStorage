//
// RPCSession 端到端测试：真实 RPCServer + 真实 TCP loopback
//
// repair: 这个文件原来挂的是【裸 syscall 手写的假服务端】，现在换成真正的 RPCServer。
//   换掉之后顺带把服务端整条链路也测了（handleClient / coRoute / co_recvRequest /
//   co_sendMessage）—— 在换之前服务端是零测试覆盖，编过但从没跑过。
//   假服务端唯一的好处是能精确控制"不回包 / 慢回包"，这里用两个假 handler（Hang / Slow）等价实现。
//
// 覆盖：
//   1. 基本往返
//   2. 连续多次调用（request_id 递增）
//   3. 非 OK 的 code 能完整传回客户端
//   4. 1MB 大响应（默认 1024 缓冲 + 多次 recv 重组）
//   5. handler 不回包 -> 客户端 100ms 超时，且耗时可观测
//   6. 迟到响应丢弃
//   7. 并发在途：抢到 m_recv_mutex 后先从缓存取
//
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
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
#include "rpc/rpcServer.hpp"

using namespace craft::RPC;

// ============================== 服务端 handler ==============================
// 全部用【具名自由函数】而不是 lambda：m_sylar 里 lambda 协程踩过 stack-use-after-return
// （协程帧被放进调用者的栈帧，调用者一 co_return 就释放）。handler 都是协程，必须避开。

// 统一回包。★ id 必须原样回 —— 客户端就是靠 it 做请求/响应路由的，忘了回就是全部超时。
static m_sylar::Task<void> sendResp(RPCSession::ptr s, int id, RpcCode code,
                                    const std::string& errmsg, const std::string& data) {
    Response resp;
    resp.set_id(id);
    resp.set_code(static_cast<int>(code));
    resp.set_errmsg(errmsg);
    resp.set_data(data);

    Frame f;
    f.setData(resp.SerializeAsString());
    co_await s->co_sendMessage(f);
    co_return;
}

static m_sylar::Task<void> hEcho(std::shared_ptr<Request> req, RPCSession::ptr s) {
    co_await sendResp(s, req->id(), RpcCode::OK, "", "echo:" + req->data());
    co_return;
}

static m_sylar::Task<void> hBig(std::shared_ptr<Request> req, RPCSession::ptr s) {
    co_await sendResp(s, req->id(), RpcCode::OK, "", std::string(1024 * 1024, 'B'));
    co_return;
}

// 故意不回 —— 测客户端超时
static m_sylar::Task<void> hHang(std::shared_ptr<Request>, RPCSession::ptr) {
    std::printf("  [server] Hang: 故意不回\n");
    co_return;
}

// 睡 200ms 再回 —— 客户端 100ms 就超时了，用来测"迟到响应被丢弃"
static m_sylar::Task<void> hSlow(std::shared_ptr<Request> req, RPCSession::ptr s) {
    std::printf("  [server] Slow: 睡 200ms 再回（客户端 100ms 就超时了）\n");
    co_await m_sylar::co_sleep(200);
    co_await sendResp(s, req->id(), RpcCode::OK, "", "late:" + req->data());
    co_return;
}

// 注意：不再需要专门注册一个"回错误码"的 handler —— 未注册的 service/method 由
// coRoute 自己回 UNK_SERVICE / UNK_METHOD（用例 3 就是测这条路由）。

// ============================== 工具 ==============================

static RPCSession::ptr g_sess;
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

// 探一个空闲端口。TcpServer / RPCServer 没有暴露"绑到 0 再读回实际端口"，只能先探一个。
static uint16_t probeFreePort() {
    const int lfd = static_cast<int>(::syscall(SYS_socket, AF_INET, SOCK_STREAM, 0));
    int on = 1;
    ::syscall(SYS_setsockopt, lfd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    if (::syscall(SYS_bind, lfd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) return 0;
    socklen_t len = sizeof(addr);
    ::syscall(SYS_getsockname, lfd, reinterpret_cast<sockaddr*>(&addr), &len);
    const uint16_t port = ntohs(addr.sin_port);
    ::syscall(SYS_close, lfd);
    return port;
}

// 封一个顺手的小工具：发一次调用并记录耗时
// repair: RPCSession::call 的返回类型已经改成 m_sylar::IOState（细粒度），这个文件还按旧的
//   CallState 写，所以编译不过。这里补一层映射，和 RPCClient::call 里的映射保持一致。
static CallState ioStateToCallState(m_sylar::IOState st) {
    switch (st) {
        case m_sylar::IOState::SUCCESS: return CallState::SUCCESS;
        case m_sylar::IOState::TIMEOUT: return CallState::TIMEOUT;
        case m_sylar::IOState::CLOSED:  return CallState::RETRY;
        default:                        return CallState::FAILED;
    }
}

static m_sylar::Task<CallState> callOnce(const std::string& service, const std::string& method,
                                         const std::string& req, std::string* resp, uint64_t* us) {
    auto r = std::make_shared<std::string>(req);
    auto p = std::make_shared<std::string>();

    const uint64_t t0 = nowUs();
    // repair: service / method 参数改成 const std::string& 了，这里直接把形参透传即可
    const CallState st = ioStateToCallState(co_await g_sess->call(service, method, r, p));
    if (us) *us = nowUs() - t0;
    if (resp) *resp = *p;
    co_return st;
}

// 用例 7 用的 A 侧调用：handler 不回包，所以它会一直占着 m_recv_mutex 直到自己超时。
// 用【具名自由函数】而不是 lambda —— 同上，lambda 协程在这个框架里有坑。
static CallState g_stateA = CallState::SUCCESS;
static uint64_t g_usA = 0;
static std::string g_respA;

static m_sylar::Task<void, m_sylar::TaskBeginExecuter> callA() {
    g_stateA = co_await callOnce("RaftRpc", "Hang", "x", &g_respA, &g_usA);
    co_return;
}

// ============================== 用例 ==============================

m_sylar::Task<void, m_sylar::TaskBeginExecuter> runAll(uint16_t port) {
    auto addr = std::dynamic_pointer_cast<m_sylar::IPv4Address>(
        m_sylar::Address::LookupAnyIPAddress("127.0.0.1"));
    addr->setPort(port);

    // ---- 起真服务端。★ 必须在本协程里构造：
    //      TcpServer 的构造参数默认实参是 IOManager::getInstance()，在 IOManager 线程外
    //      构造会拿到 nullptr，后面 getIomanager()->schedule() 就空指针。
    auto server = std::make_shared<RPCServer>();
    if (!server->bind(addr)) {
        check(false, "0. RPCServer bind + listen 成功", "port=" + std::to_string(port));
        g_done.store(true);
        co_return;
    }
    server->registeRoute("RaftRpc", "AppendEntries", hEcho);
    server->registeRoute("RaftRpc", "RequestVote",   hEcho);
    server->registeRoute("RaftRpc", "Big",           hBig);
    server->registeRoute("RaftRpc", "Hang",          hHang);
    server->registeRoute("RaftRpc", "Slow",          hSlow);
    server->start();

    // ---- 建连：必须用 co_connect（Socket::connect 在 m_sylar 里不可用）----
    auto sock = m_sylar::Socket::CreateTCP(addr);
    if (!sock || !sock->isValid()) {
        check(false, "0. 客户端 socket 创建", "");
        g_done.store(true);
        co_return;
    }
    const int fd = sock->getFd();
    const int rt = co_await m_sylar::co_connect(fd, addr->getAddr(), addr->getAddrLen());
    if (rt != 0) {
        check(false, "0. co_connect", std::string("rt=") + std::to_string(rt) + " errno=" +
                                          std::string(std::strerror(errno)));
        g_done.store(true);
        co_return;
    }
    sock->init(fd);                 // 置 m_isConnected
    g_sess = std::make_shared<RPCSession>(sock);
    g_sess->setNodeId(1);

    std::string resp;
    uint64_t us = 0;

    // ---- 1. 基本往返 ----
    CallState st = co_await callOnce("RaftRpc", "AppendEntries", "hello", &resp, &us);
    check(st == CallState::SUCCESS && resp == "echo:hello", "1. 基本往返（真 server echo）",
          "state=" + std::to_string(static_cast<int>(st)) + " resp=" + resp +
              " us=" + std::to_string(us));

    // ---- 2. 连续 5 次调用 ----
    {
        bool allOk = true;
        std::string detail;
        for (int i = 0; i < 5; ++i) {
            const std::string body = "seq-" + std::to_string(i);
            st = co_await callOnce("RaftRpc", "RequestVote", body, &resp, &us);
            if (st != CallState::SUCCESS || resp != "echo:" + body) {
                allOk = false;
                detail = "i=" + std::to_string(i) + " state=" + std::to_string(static_cast<int>(st)) +
                         " resp=" + resp;
                break;
            }
        }
        check(allOk, "2. 连续 5 次调用", detail);
    }

    // ---- 3. 未注册的 method / service：coRoute 必须【立刻回错误码】，不能干等超时 ----
    //   ★ 这条是关键回归：修之前 coRoute 只打日志、不回包，客户端要干等一个完整的
    //   100ms recv 超时才失败；而 RPCClient::call 连续 3 次超时就把这条【健康】连接
    //   判成 CLOSED 去重连 —— 也就是 service/method 名字打错一个字母就会引发重连 churn。
    //   现在服务端立刻回 UNK_METHOD/UNK_SERVICE，所以断言"收到了帧"且"耗时远小于 100ms"。
    st = co_await callOnce("RaftRpc", "NoSuchMethod", "x", &resp, &us);
    check(st == CallState::SUCCESS && us < 50 * 1000,
          "3. 未注册的 method -> 服务端立刻回 UNK_METHOD（不再干等到超时）",
          "state=" + std::to_string(static_cast<int>(st)) + " us=" + std::to_string(us) +
              " (期望 SUCCESS 且 <50ms；若 ≈100000 说明服务端没回包)");

    st = co_await callOnce("NoSuchService", "Whatever", "x", &resp, &us);
    check(st == CallState::SUCCESS && us < 50 * 1000,
          "3b. 未注册的 service -> 服务端立刻回 UNK_SERVICE",
          "state=" + std::to_string(static_cast<int>(st)) + " us=" + std::to_string(us));

    // ---- 4. 1MB 大响应 ----
    st = co_await callOnce("RaftRpc", "Big", "big", &resp, &us);
    std::printf("     [info] 1MB 响应耗时 %llu us（Session buffer 默认 1024 字节）\n",
                static_cast<unsigned long long>(us));
    check(st == CallState::SUCCESS && resp.size() == 1024 * 1024 && resp[0] == 'B',
          "4. 1MB 大响应（默认 1024 缓冲 + 多次 recv 重组）",
          "state=" + std::to_string(static_cast<int>(st)) + " size=" + std::to_string(resp.size()) +
              " us=" + std::to_string(us));

    // ---- 5. handler 不回包 -> 超时 ----
    st = co_await callOnce("RaftRpc", "Hang", "x", &resp, &us);
    check((st == CallState::TIMEOUT || st == CallState::FAILED) && us >= 90 * 1000 && us <= 400 * 1000,
          "5. handler 不回包 -> 超时（耗时应在 ~100ms）",
          "state=" + std::to_string(static_cast<int>(st)) + " us=" + std::to_string(us) +
              " (期望 TIMEOUT=6 或 FAILED=5, us≈100000)");

    // ---- 6. 迟到响应丢弃 ----
    // 先发一个必然超时的 Slow（server 200ms 后才回），紧接着正常调用。
    // 正常调用读到 Slow 的迟到响应时 id 不匹配，必须丢弃并继续等自己的。
    st = co_await callOnce("RaftRpc", "Slow", "s", &resp, &us);
    const CallState slowState = st;
    st = co_await callOnce("RaftRpc", "AppendEntries", "after-slow", &resp, &us);
    check(st == CallState::SUCCESS && resp == "echo:after-slow", "6. 迟到响应被丢弃",
          "slow_state=" + std::to_string(static_cast<int>(slowState)) +
              " next_state=" + std::to_string(static_cast<int>(st)) + " resp=" + resp);

    // ---- 7. 并发在途：抢到 m_recv_mutex 之后必须【先去缓存里拿】 ----
    //   A 先发一个 server 永远不回的 Hang，于是 A 会一直占着 m_recv_mutex 等自己的 id。
    //   紧接着 B 发一个正常请求：B 的响应会被 A 从 socket 读出来、缓存进 m_recved_frames，
    //   而此时 B 正阻塞在 m_recv_mutex 上。等 A 超时放开锁，B 应该【立刻】从缓存里拿到答案。
    //   如果 co_recvResponse 抢到锁之后不重新查一次缓存、直接闷头 recvMessage，
    //   B 就会白等一个完整的 100ms recv 超时，最后错误地报 TIMEOUT。
    {
        m_sylar::IOManager::getInstance()->schedule(m_sylar::TaskCoro20::create_coro(&callA));
        co_await m_sylar::co_sleep(30);     // 等 A 把请求发出去、进到 recvMessage（此时已持有锁）

        uint64_t us_B = 0;
        const CallState st_B = co_await callOnce("RaftRpc", "AppendEntries", "inflight", &resp, &us_B);
        check(st_B == CallState::SUCCESS && resp == "echo:inflight",
              "7. \xe2\x98\x85 并发在途：抢到锁后先从缓存取，不能白等一次 recv 超时",
              "B: state=" + std::to_string(static_cast<int>(st_B)) + " resp=" + resp +
                  " us=" + std::to_string(us_B) + " | A: state=" +
                  std::to_string(static_cast<int>(g_stateA)) + " us=" + std::to_string(g_usA));
    }

    g_done.store(true);
    co_return;
}

// ============================== main ==============================

int main() {
    // std::_Exit 不刷新 stdio 缓冲 —— 输出重定向到文件时最后一段结果会整段丢失，这里关掉缓冲
    setvbuf(stdout, nullptr, _IONBF, 0);
    setvbuf(stderr, nullptr, _IONBF, 0);

    const uint16_t port = probeFreePort();
    if (port == 0) {
        std::printf("探测空闲端口失败\n");
        return 1;
    }
    std::printf("真 RPCServer 将监听 127.0.0.1:%u\n\n", port);

    auto* iom = new m_sylar::IOManager("rpc_session_test", 1);
    iom->schedule(m_sylar::TaskCoro20::create_coro(std::bind(runAll, port)));

    // 等全部用例跑完（最多 10s）
    for (int i = 0; i < 200; ++i) {
        if (g_done.load()) break;
        ::usleep(50 * 1000);
    }
    if (!g_done.load()) {
        std::printf("  \xe2\x9c\x97 0. 用例没跑完（超时 10s）\n");
        ++g_fail;
    }

    std::printf("\n结果: pass=%d fail=%d\n", g_pass, g_fail);

    // ★ 刻意跳过正常析构：
    //   IOManager::getInstance() == dynamic_cast<IOManager*>(Scheduler::GetThis())，
    //   在非 IOManager 线程上返回 nullptr，而 ~Socket() 会调它的 closeFd() → 空指针。
    std::_Exit(g_fail == 0 ? 0 : 1);
}
