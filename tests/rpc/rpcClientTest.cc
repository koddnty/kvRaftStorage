//
// RPCClient 重连语义测试：真实 TCP loopback + 一个可以上下线的假服务端
//
// 专门盯本次修掉的那三个坑（都是"对端掉线"才暴露的）：
//   1. 对端掉线后 call() 必须【快速失败】返回 RETRY，绝不能卡住 ——
//      旧实现里重连协程持着 m_infos[id].mutex 在 `while(rt)` 里死循环，而 call() 的
//      CLOSED 分支要 co_await 拿同一把锁，于是对该节点的所有 call() 永久阻塞。
//      Raft 里心跳协程就是这么被卡死的，leader 直接被选掉。
//   2. coConnect() 的重连尝试必须【封顶】后放弃，不能无限重试。
//   3. 对端恢复后必须能重连成功、调用恢复正常（整体替换 RPCSession 那条路径真能用）。
//
// 整个用例挂了 alarm() 看门狗：一旦卡死就直接退出并打标记 —— 卡死本身就是第 1 条的证据。
//
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <csignal>
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

// ============================== 假服务端 ==============================
// 用裸 syscall，避开 m_sylar 的 fd 管理（客户端已经占用了那些 Hook）

static ssize_t rawRead(int fd, void* buf, size_t n) { return ::syscall(SYS_read, fd, buf, n); }
static ssize_t rawWrite(int fd, const void* buf, size_t n) { return ::syscall(SYS_write, fd, buf, n); }

static std::atomic<int> g_mode{0};      // 0 = 下线（不监听、断开已有连接），1 = 上线
static std::atomic<int> g_quit{0};
static uint16_t g_port = 0;

static int makeListener(uint16_t port) {
    const int lfd = static_cast<int>(::syscall(SYS_socket, AF_INET, SOCK_STREAM, 0));
    if (lfd < 0) return -1;
    int on = 1;
    ::syscall(SYS_setsockopt, lfd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(port);
    if (::syscall(SYS_bind, lfd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        ::syscall(SYS_close, lfd);
        return -1;
    }
    ::syscall(SYS_listen, lfd, 8);
    return lfd;
}

// 带 20ms 超时的读：既能攒满 n 字节，也能及时响应 g_mode 变化（否则会一直阻塞在 read 上，
// 没法模拟"对端下线"）。返回值：true = 读满，false = 超时/EOF/错误/被要求下线。
static bool readN(int fd, void* buf, size_t n) {
    size_t got = 0;
    while (got < n) {
        if (g_mode.load() == 0) return false;
        const ssize_t r = rawRead(fd, static_cast<char*>(buf) + got, n - got);
        if (r > 0) {
            got += static_cast<size_t>(r);
            continue;
        }
        if (r < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) continue;   // 超时，再转一圈看 mode
        return false;                                                       // EOF 或真错误
    }
    return true;
}

static void serveConn(int cfd) {
    while (g_mode.load() == 1) {
        unsigned char hdr[4];
        if (!readN(cfd, hdr, 4)) return;
        const uint32_t len = (uint32_t(hdr[0]) << 24) | (uint32_t(hdr[1]) << 16) |
                             (uint32_t(hdr[2]) << 8) | uint32_t(hdr[3]);
        std::string payload(len, '\0');
        if (len > 0 && !readN(cfd, payload.data(), len)) return;

        Request req;
        if (!req.ParseFromString(payload)) return;

        Response resp;
        resp.set_id(req.id());
        resp.set_code(0);
        resp.set_data("echo:" + req.data());

        std::string body;
        resp.SerializeToString(&body);
        const std::string wire = Frame(body).toWire();
        if (rawWrite(cfd, wire.data(), wire.size()) != static_cast<ssize_t>(wire.size())) return;
    }
}

static void serverLoop() {
    while (!g_quit.load()) {
        if (g_mode.load() == 0) {
            ::usleep(10 * 1000);
            continue;
        }
        const int lfd = makeListener(g_port);
        if (lfd < 0) {
            ::usleep(10 * 1000);
            continue;
        }
        const int cfd = static_cast<int>(::syscall(SYS_accept, lfd, nullptr, nullptr));
        if (cfd >= 0) {
            timeval tv{};
            tv.tv_usec = 20 * 1000;
            ::syscall(SYS_setsockopt, cfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
            serveConn(cfd);
            ::syscall(SYS_close, cfd);
        }
        ::syscall(SYS_close, lfd);      // 下线时把 listener 也关掉，新连接才会被拒
    }
}

// ============================== 客户端 ==============================

static RPCClient::ptr g_client;
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

// 发一次调用并记录耗时
static m_sylar::Task<CallState> doCall(const std::string& body, std::string* resp, uint64_t* us) {
    auto s = std::make_shared<std::string>("RaftRpc");
    auto mth = std::make_shared<std::string>("AppendEntries");
    auto r = std::make_shared<std::string>(body);
    auto p = std::make_shared<std::string>();

    const uint64_t t0 = nowUs();
    const CallState st = co_await g_client->call(0, s, mth, r, p);
    if (us) *us = nowUs() - t0;
    if (resp) *resp = *p;
    co_return st;
}

static m_sylar::Task<void, m_sylar::TaskBeginExecuter> runAll() {
    auto addr = std::dynamic_pointer_cast<m_sylar::IPv4Address>(
        m_sylar::Address::LookupAnyIPAddress("127.0.0.1"));
    addr->setPort(g_port);

    // ---------- 1. 正常建连 + 往返 ----------
    const int id = g_client->addPeer(addr);
    check(id == 0, "1a. addPeer 分配 node id（从 0 开始）", "id=" + std::to_string(id));

    std::string resp;
    uint64_t us = 0;
    const int crt = co_await g_client->coConnect(id);
    check(crt == 0, "1b. coConnect 成功", "rt=" + std::to_string(crt));

    CallState st = co_await doCall("hello", &resp, &us);
    check(st == CallState::SUCCESS && resp == "echo:hello", "1c. 调用成功",
          "state=" + stateName(st) + " resp=" + resp + " us=" + std::to_string(us));

    // 已连接时 coConnect 应该直接返回，不该白跑一轮 connect
    const uint64_t t_re = nowUs();
    const int crt2 = co_await g_client->coConnect(id);
    const uint64_t re_us = nowUs() - t_re;
    check(crt2 == 0 && re_us < 20 * 1000, "1d. 已连接时 coConnect 立刻返回",
          "rt=" + std::to_string(crt2) + " us=" + std::to_string(re_us));

    // ---------- 2. 对端下线 ----------
    g_mode.store(0);                    // 服务端断开连接并关掉 listener
    co_await m_sylar::co_sleep(80);     // 等 FIN/RST 传到客户端

    uint64_t t_down = 0;
    st = co_await doCall("after-down", &resp, &t_down);
    check(st != CallState::SUCCESS && t_down < 500 * 1000,
          "2a. 掉线后第一次调用快速失败（不 hang）",
          "state=" + stateName(st) + " us=" + std::to_string(t_down));

    // ★ 本文件的核心回归用例。
    //   旧实现：call() 的 CLOSED 分支要 co_await 拿 m_infos[0].mutex，而重连协程正持着那把锁
    //   在 `while(rt)` 里死循环 → 下面这三次调用会【永久阻塞】，看门狗会把它打出来。
    //   现在：state==false 时 call() 只读一个 atomic 就返回 RETRY，不碰任何锁。
    bool fast = true;
    std::string detail;
    for (int i = 0; i < 3; ++i) {
        uint64_t t1 = 0;
        const CallState s2 = co_await doCall("dead-" + std::to_string(i), &resp, &t1);
        if (s2 != CallState::RETRY || t1 >= 30 * 1000) {
            fast = false;
            detail = "i=" + std::to_string(i) + " state=" + stateName(s2) +
                     " us=" + std::to_string(t1) + " (期望 RETRY 且 <30ms)";
            break;
        }
    }
    check(fast, "2b. \xe2\x98\x85 掉线后连续 3 次调用都立刻返回 RETRY（不再被重连协程的锁卡死）", detail);

    // ---------- 3. 对端不在线时 coConnect 必须有限次尝试后放弃 ----------
    co_await m_sylar::co_sleep(150);    // 等 2a 触发的那个重连协程跑完（3 次尝试 + 退避 ≈ 15ms）
    const uint64_t t_rec = nowUs();
    const int rrt = co_await g_client->coConnect(id);
    const uint64_t rec_us = nowUs() - t_rec;
    check(rrt != 0 && rec_us < 3 * 1000 * 1000,
          "3a. \xe2\x98\x85 对端不在线时 coConnect 有限次尝试后放弃（不再 while(rt) 死循环）",
          "rt=" + std::to_string(rrt) + " us=" + std::to_string(rec_us) + " (期望 rt!=0 且 <3s)");

    st = co_await doCall("still-down", &resp, &us);
    check(st == CallState::RETRY, "3b. 放弃重连后，调用依然立刻返回 RETRY",
          "state=" + stateName(st) + " us=" + std::to_string(us));

    // ---------- 4. 对端恢复 ----------
    g_mode.store(1);
    co_await m_sylar::co_sleep(80);     // 等服务端重新 listen

    const int crt3 = co_await g_client->coConnect(id);
    check(crt3 == 0, "4a. 对端恢复后 coConnect 成功（整体替换 RPCSession）",
          "rt=" + std::to_string(crt3));

    st = co_await doCall("back", &resp, &us);
    check(st == CallState::SUCCESS && resp == "echo:back", "4b. 重连后调用恢复正常",
          "state=" + stateName(st) + " resp=" + resp + " us=" + std::to_string(us));

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
    // std::_Exit 不刷新 stdio 缓冲，这里关掉缓冲免得输出整段丢失
    setvbuf(stdout, nullptr, _IONBF, 0);
    setvbuf(stderr, nullptr, _IONBF, 0);

    // 先探一个空闲端口留给假服务端反复上线/下线用（上线/下线都绑同一个端口）
    {
        uint16_t probe = 0;
        const int tmp = makeListener(0);
        if (tmp < 0) {
            std::printf("探测端口失败\n");
            return 1;
        }
        sockaddr_in addr{};
        socklen_t len = sizeof(addr);
        ::syscall(SYS_getsockname, tmp, reinterpret_cast<sockaddr*>(&addr), &len);
        probe = ntohs(addr.sin_port);
        ::syscall(SYS_close, tmp);
        g_port = probe;
    }

    ::signal(SIGALRM, onAlarm);
    ::alarm(20);

    g_client = std::make_shared<RPCClient>();
    g_mode.store(1);
    std::thread srv(serverLoop);
    std::printf("假服务端 127.0.0.1:%u（可上下线）\n\n", g_port);

    auto* iom = new m_sylar::IOManager("rpc_client_test", 1);
    iom->schedule(m_sylar::TaskCoro20::create_coro(runAll));

    // 等 10 个用例跑完（最多 15s，超过就让看门狗兜底）
    for (int i = 0; i < 300; ++i) {
        if ((g_pass + g_fail) >= 10) break;
        ::usleep(50 * 1000);
    }

    std::printf("\n结果: pass=%d fail=%d\n", g_pass, g_fail);

    // ★ 刻意跳过正常析构（理由同 rpcSessionTest）：
    //   IOManager::getInstance() 在非 IOManager 线程上返回 nullptr，而 ~Socket() 会调它的
    //   closeFd() → 空指针。
    g_quit.store(1);
    g_mode.store(0);
    ::alarm(0);
    std::_Exit(g_fail == 0 ? 0 : 1);
}
