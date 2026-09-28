//
// RPCSession 端到端测试：真实 TCP loopback + 假服务端
//
// 这是 client 第一次真正跑起来 —— 覆盖：
//   1. 基本往返
//   2. 连续多次调用（request_id 递增）
//   3. 服务端错误码映射（RpcCode -> CallState）
//   4. 1MB 大响应（验证 setBufferSize + 多次 recv 重组）
//   5. 服务端不响应 -> 超时，且耗时可观测
//   6. 迟到响应丢弃（验证 goto retry）
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
#include <string>
#include <thread>

#include <sylar/basic/address.h>
#include <sylar/basic/socket.h>
#include <sylar/coroutine/corobase.h>
#include <sylar/coroutine/coro20/hook.h>

#include "rpc/frame.hpp"
#include "rpc/payload.pb.h"
#include "rpc/rpcClient.hpp"

using namespace craft::RPC;

// ============================== 服务端 ==============================
// 用裸 syscall，避开 m_sylar 的 fd 管理

static ssize_t rawRead(int fd, void* buf, size_t n) { return ::syscall(SYS_read, fd, buf, n); }
static ssize_t rawWrite(int fd, const void* buf, size_t n) { return ::syscall(SYS_write, fd, buf, n); }

static bool readAll(int fd, void* buf, size_t n) {
    size_t got = 0;
    while (got < n) {
        const ssize_t r = rawRead(fd, static_cast<char*>(buf) + got, n - got);
        if (r <= 0) return false;
        got += static_cast<size_t>(r);
    }
    return true;
}

static bool writeAll(int fd, const void* buf, size_t n) {
    size_t sent = 0;
    while (sent < n) {
        const ssize_t r = rawWrite(fd, static_cast<const char*>(buf) + sent, n - sent);
        if (r <= 0) return false;
        sent += static_cast<size_t>(r);
    }
    return true;
}

static int listenOn(uint16_t* out_port) {
    const int lfd = static_cast<int>(::syscall(SYS_socket, AF_INET, SOCK_STREAM, 0));
    int on = 1;
    ::syscall(SYS_setsockopt, lfd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    if (::syscall(SYS_bind, lfd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) return -1;
    ::syscall(SYS_listen, lfd, 8);

    socklen_t len = sizeof(addr);
    ::syscall(SYS_getsockname, lfd, reinterpret_cast<sockaddr*>(&addr), &len);
    *out_port = ntohs(addr.sin_port);
    return lfd;
}

static std::atomic<int> g_stop{0};

static void serverLoop(int lfd) {
    const int cfd = static_cast<int>(::syscall(SYS_accept, lfd, nullptr, nullptr));
    if (cfd < 0) return;

    while (!g_stop.load()) {
        unsigned char hdr[4];
        if (!readAll(cfd, hdr, 4)) break;
        const uint32_t len = (uint32_t(hdr[0]) << 24) | (uint32_t(hdr[1]) << 16) |
                             (uint32_t(hdr[2]) << 8) | uint32_t(hdr[3]);

        std::string payload(len, '\0');
        if (len > 0 && !readAll(cfd, payload.data(), len)) break;

        Request req;
        if (!req.ParseFromString(payload)) {
            std::printf("  [server] 请求解析失败\n");
            break;
        }

        Response resp;
        resp.set_id(req.id());
        const std::string& m = req.method();

        if (m == "NoSuchMethod") {
            resp.set_code(static_cast<int>(RpcCode::UNK_METHOD));
            resp.set_errmsg("server has no such method");
        } else if (m == "Hang") {
            std::printf("  [server] Hang: 故意不回\n");
            continue;                       // 不回，测客户端超时
        } else if (m == "Slow") {
            std::printf("  [server] Slow: 睡 200ms 再回（客户端 100ms 就超时了）\n");
            ::usleep(200 * 1000);
            resp.set_code(0);
            resp.set_data("late:" + req.data());
        } else if (m == "Big") {
            resp.set_code(0);
            resp.set_data(std::string(1024 * 1024, 'B'));
        } else {
            resp.set_code(0);
            resp.set_data("echo:" + req.data());
        }

        std::string body;
        resp.SerializeToString(&body);
        const std::string wire = Frame(body).toWire();
        if (!writeAll(cfd, wire.data(), wire.size())) break;
    }
    ::syscall(SYS_close, cfd);
}

// ============================== 客户端 ==============================

static RPCSession::ptr g_sess;
static int g_pass = 0;
static int g_fail = 0;

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

// 封一个顺手的小工具：发一次调用并记录耗时
// repair: RPCSession::call 的返回类型已经改成 m_sylar::IOState（细粒度，见 rpcClient.hpp 顶部
//   的说明），这个测试还按旧的 CallState 写，所以整个文件编译不过。这里补一层映射，
//   和 RPCClient::call 里的映射保持一致（CLOSED / TIMEOUT 各自区分，不再一律压成 FAILED）。
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
    auto s = std::make_shared<std::string>(service);
    auto mth = std::make_shared<std::string>(method);
    auto r = std::make_shared<std::string>(req);
    auto p = std::make_shared<std::string>();

    const uint64_t t0 = nowUs();
    const CallState st = ioStateToCallState(co_await g_sess->call(s, mth, r, p));
    if (us) *us = nowUs() - t0;
    if (resp) *resp = *p;
    co_return st;
}

// 用例 7 用的 A 侧调用：服务端不回，所以它会一直占着 m_recv_mutex 直到自己超时。
// 用【具名自由函数】而不是 lambda —— 前面已经被 lambda 协程的 stack-use-after-return 咬过一次。
static CallState g_stateA = CallState::SUCCESS;
static uint64_t g_usA = 0;
static std::string g_respA;

static m_sylar::Task<void, m_sylar::TaskBeginExecuter> callA() {
    g_stateA = co_await callOnce("RaftRpc", "Hang", "x", &g_respA, &g_usA);
    co_return;
}

m_sylar::Task<void, m_sylar::TaskBeginExecuter> runAll(uint16_t port, bool* connected) {
    // ---- 建连：必须用 co_connect（Socket::connect 在 m_sylar 里不可用）----
    auto addr = std::dynamic_pointer_cast<m_sylar::IPv4Address>(
        m_sylar::Address::LookupAnyIPAddress("127.0.0.1"));
    addr->setPort(port);

    auto sock = m_sylar::Socket::CreateTCP(addr);
    if (!sock || !sock->isValid()) {
        *connected = false;
        co_return;
    }
    const int fd = sock->getFd();
    const int rt = co_await m_sylar::co_connect(fd, addr->getAddr(), addr->getAddrLen());
    if (rt != 0) {
        std::printf("!! co_connect 失败 rt=%d errno=%d(%s)\n", rt, errno, std::strerror(errno));
        *connected = false;
        co_return;
    }
    sock->init(fd);                 // 置 m_isConnected
    g_sess = std::make_shared<RPCSession>(sock);
    g_sess->setNodeId(1);
    *connected = true;

    std::string resp;
    uint64_t us = 0;

    // ---- 1. 基本往返 ----
    CallState st = co_await callOnce("RaftRpc", "AppendEntries", "hello", &resp, &us);
    check(st == CallState::SUCCESS && resp == "echo:hello", "1. 基本往返",
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

    // ---- 3. 服务端错误码 ----
    // repair: 这条原来断言 CallState::UNK_METHOD，但 RpcCode -> CallState 的映射（codeToState）
    //   在 RPCClient::call 里，RPCSession::call 只负责把响应帧交出来、完全不解释 code，
    //   所以在这一层拿不到 UNK_METHOD。这里改成断言传输层的事实：非 OK 的 code 也能完整收到帧。
    //   code 映射本身要靠 RPCClient 的端到端测试覆盖（等 init() 的配置装载接上之后）。
    st = co_await callOnce("RaftRpc", "NoSuchMethod", "x", &resp, &us);
    check(st == CallState::SUCCESS, "3. 非 OK 的 code 也能收到完整帧（code 映射在 RPCClient 层）",
          "state=" + std::to_string(static_cast<int>(st)) +
              " (RPCSession 不做 RpcCode->CallState 映射)");

    // ---- 4. 1MB 大响应 ----
    // 注意：服务端只在 method == "Big" 时返回 1MB（写错方法名会退化成 echo，静默假通过）
    st = co_await callOnce("RaftRpc", "Big", "big", &resp, &us);
    std::printf("     [info] 1MB 响应耗时 %llu us（Session buffer 默认 1024 字节）\n",
                static_cast<unsigned long long>(us));
    check(st == CallState::SUCCESS && resp.size() == 1024 * 1024 && resp[0] == 'B',
          "4. 1MB 大响应（默认 1024 缓冲 + 多次 recv 重组）",
          "state=" + std::to_string(static_cast<int>(st)) + " size=" + std::to_string(resp.size()) +
              " us=" + std::to_string(us));

    // ---- 5. 服务端不响应 -> 超时 ----
    st = co_await callOnce("RaftRpc", "Hang", "x", &resp, &us);
    check((st == CallState::TIMEOUT || st == CallState::FAILED) && us >= 90 * 1000 && us <= 400 * 1000,
          "5. 服务端不响应 -> 超时（耗时应在 ~100ms）",
          "state=" + std::to_string(static_cast<int>(st)) + " us=" + std::to_string(us) +
              " (期望 TIMEOUT=6 或 FAILED=5, us≈100000)");

    // ---- 6. 迟到响应丢弃 ----
    // 先发一个必然超时的 Slow（服务端 200ms 后才回），紧接着正常调用。
    // 正常调用读到 Slow 的迟到响应时 id 不匹配，必须丢弃并继续等自己的。
    st = co_await callOnce("RaftRpc", "Slow", "s", &resp, &us);
    const CallState slowState = st;
    st = co_await callOnce("RaftRpc", "AppendEntries", "after-slow", &resp, &us);
    check(st == CallState::SUCCESS && resp == "echo:after-slow", "6. 迟到响应被丢弃（goto retry）",
          "slow_state=" + std::to_string(static_cast<int>(slowState)) +
              " next_state=" + std::to_string(static_cast<int>(st)) + " resp=" + resp);

    // ---- 7. 并发在途：抢到 m_recv_mutex 之后必须【先去缓存里拿】 ----
    //   A 先发一个服务端永远不回的 Hang，于是 A 会一直占着 m_recv_mutex 等自己的 id。
    //   紧接着 B 发一个正常请求：B 的响应会被 A 从 socket 读出来、缓存进 m_recved_frames，
    //   而此时 B 正阻塞在 m_recv_mutex 上。等 A 超时放开锁，B 应该【立刻】从缓存里拿到答案。
    //   如果 co_recvResponse 抢到锁之后不重新查一次缓存、直接闷头 recvMessage，
    //   B 就会白等一个完整的 100ms recv 超时，最后错误地报 TIMEOUT ——
    //   而 RPCClient::call 那边连续 3 次超时就会被当成"连接断了"去重连，连锁反应。
    {
        m_sylar::IOManager::getInstance()->schedule(m_sylar::TaskCoro20::create_coro(&callA));
        // 等 A 把请求发出去、进到 recvMessage（此时它已经持有 m_recv_mutex）
        co_await m_sylar::co_sleep(30);

        uint64_t us_B = 0;
        const CallState st_B = co_await callOnce("RaftRpc", "AppendEntries", "inflight", &resp, &us_B);
        check(st_B == CallState::SUCCESS && resp == "echo:inflight",
              "7. \xe2\x98\x85 并发在途：抢到锁后先从缓存取，不能白等一次 recv 超时",
              "B: state=" + std::to_string(static_cast<int>(st_B)) + " resp=" + resp +
                  " us=" + std::to_string(us_B) + " | A: state=" +
                  std::to_string(static_cast<int>(g_stateA)) + " us=" + std::to_string(g_usA) +
                  "（B 的响应早就在缓存里了，应该在 A 放开锁的瞬间返回；"
                  "如果 us≈200000 且 state=TIMEOUT 就是这个 bug）");
    }

    co_return;
}

// ============================== main ==============================

int main() {
    // std::_Exit 不刷新 stdio 缓冲 —— 输出重定向到文件时最后一段结果会整段丢失，这里关掉缓冲
    setvbuf(stdout, nullptr, _IONBF, 0);
    setvbuf(stderr, nullptr, _IONBF, 0);

    uint16_t port = 0;
    const int lfd = listenOn(&port);
    if (lfd < 0) {
        std::printf("listen 失败\n");
        return 1;
    }

    std::thread srv(serverLoop, lfd);
    std::printf("假服务端 127.0.0.1:%u\n\n", port);

    bool connected = false;
    auto* iom = new m_sylar::IOManager("rpc_session_test", 1);
    iom->schedule(m_sylar::TaskCoro20::create_coro(std::bind(runAll, port, &connected)));

    // 等全部用例跑完（最多 10s）
    for (int i = 0; i < 200; ++i) {
        if (connected && (g_pass + g_fail) >= 7) break;
        ::usleep(50 * 1000);
    }

    std::printf("\n结果: pass=%d fail=%d\n", g_pass, g_fail);

    // ★ 刻意跳过正常析构：
    //   IOManager::getInstance() == dynamic_cast<IOManager*>(Scheduler::GetThis())，
    //   在非 IOManager 线程上返回 nullptr，而 ~Socket() 会调它的 closeFd() → 空指针。
    //   服务端线程此时也阻塞在 read 上。
    g_stop.store(1);
    std::_Exit(g_fail == 0 ? 0 : 1);
}
