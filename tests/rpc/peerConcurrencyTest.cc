//
// Peer 并发调用测试：验证 "各发各的 + 单 driver 收帧" 的分发逻辑
//
// 场景：N 个协程同时对同一个 Peer 发请求，服务端每个请求延迟 30ms，
//       保证多个请求同时在途 —— 这正是 driver/waiter 逻辑唯一会被走到的路径。
//
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <atomic>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include <sylar/basic/address.h>
#include <sylar/basic/socket.h>
#include <sylar/coroutine/corobase.h>
#include <sylar/coroutine/coro20/hook.h>

#include "rpc/rpcClient.hpp"

using namespace craft::RPC;

// ---------------------------------------------------------------- 服务端
// 用裸 syscall：m_sylar hook 了 read/write/accept/socket，在非协程线程里会出问题
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

// 起一个监听 socket，返回 fd；端口写进 out_port
static int listenOn(uint16_t* out_port) {
    const int lfd = static_cast<int>(::syscall(SYS_socket, AF_INET, SOCK_STREAM, 0));
    int on = 1;
    ::syscall(SYS_setsockopt, lfd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;  // 让内核挑端口
    if (::syscall(SYS_bind, lfd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) return -1;
    ::syscall(SYS_listen, lfd, 8);

    socklen_t len = sizeof(addr);
    ::syscall(SYS_getsockname, lfd, reinterpret_cast<sockaddr*>(&addr), &len);
    *out_port = ntohs(addr.sin_port);
    return lfd;
}

static constexpr int kServerDelayMs = 30;
static std::atomic<int> g_served{0};
static std::atomic<int> g_die_after{0};   // >0：服务端处理这么多条后直接断开（测失败路径）

static void serverLoop(int lfd, int expect) {
    const int cfd = static_cast<int>(::syscall(SYS_accept, lfd, nullptr, nullptr));
    if (cfd < 0) return;

    while (g_served.load() < expect) {
        // 读 4 字节大端长度头
        unsigned char hdr[4];
        if (!readAll(cfd, hdr, 4)) break;
        const uint32_t len = (uint32_t(hdr[0]) << 24) | (uint32_t(hdr[1]) << 16) |
                             (uint32_t(hdr[2]) << 8) | uint32_t(hdr[3]);

        std::string payload(len, '\0');
        if (len > 0 && !readAll(cfd, payload.data(), len)) break;

        Request req;
        if (!req.ParseFromString(payload)) break;

        // ★ 故意延迟：让多个请求同时在途，逼出 driver/waiter 分发路径
        ::usleep(kServerDelayMs * 1000);

        Response resp;
        resp.set_id(req.id());
        resp.set_code(0);
        resp.set_data("echo:" + req.data());

        std::string body;
        resp.SerializeToString(&body);
        const std::string wire = Frame(body).toWire();
        if (!writeAll(cfd, wire.data(), wire.size())) break;

        const int served = g_served.fetch_add(1) + 1;

        // 失败路径：处理够指定条数就断开，剩下的请求永远等不到响应
        const int die = g_die_after.load();
        if (die > 0 && served >= die) {
            std::printf("  [server] 断开连接（已处理 %d 条）\n", served);
            break;
        }
    }
    ::syscall(SYS_close, cfd);
}

// ---------------------------------------------------------------- 客户端
static constexpr int kClients = 6;

static std::atomic<int> g_ok{0};
static std::atomic<int> g_bad{0};

m_sylar::Task<void, m_sylar::TaskBeginExecuter> oneCall(Peer::ptr peer, int i) {
    auto svc = std::make_shared<std::string>("RaftRpc");
    auto mth = std::make_shared<std::string>("AppendEntries");
    auto req = std::make_shared<std::string>("payload-" + std::to_string(i));
    auto resp = std::make_shared<std::string>();

    const CallState st = co_await peer->call(svc, mth, req, resp);

    const std::string want = "echo:payload-" + std::to_string(i);
    if (st == CallState::SUCCESS && *resp == want) {
        std::printf("  [call %d] SUCCESS   resp=%s\n", i, resp->c_str());
        g_ok.fetch_add(1);
    } else {
        std::printf("  [call %d] FAILED    state=%d resp=%s (want %s)\n", i, static_cast<int>(st),
                    resp->c_str(), want.c_str());
        g_bad.fetch_add(1);
    }
    co_return;
}

m_sylar::Task<void, m_sylar::TaskBeginExecuter> connectPeer(Peer::ptr peer, uint16_t port, bool* ok) {
    auto addr = std::dynamic_pointer_cast<m_sylar::IPv4Address>(
        m_sylar::Address::LookupAnyIPAddress("127.0.0.1"));
    addr->setPort(port);

    peer->m_sock = m_sylar::Socket::CreateTCP(addr);
    if (!peer->m_sock || !peer->m_sock->isValid()) {
        *ok = false;
        co_return;
    }

    // ★ 关键：Socket::connect() 内部用的是裸 ::connect()，而 m_sylar 的 socket 是非阻塞的，
    //   必然返回 EINPROGRESS → Socket::connect() 在这个框架里【不可用】。
    //   必须走协程版 co_connect。
    const int fd = peer->m_sock->getFd();
    const int rt = co_await m_sylar::co_connect(fd, addr->getAddr(), addr->getAddrLen());
    if (rt != 0) {
        std::printf("!! co_connect failed, rt=%d errno=%d(%s)\n", rt, errno, std::strerror(errno));
        *ok = false;
        co_return;
    }

    peer->m_sock->init(fd);              // 让 Socket 知道已连上（send/recv 会检查 m_isConnected）
    peer->m_sock->setRecvTimeOut(500 * 1000);
    peer->m_sock->setSendTimeOut(500 * 1000);
    peer->m_state.store(PeerState::READY);
    *ok = true;
    co_return;
}

int main(int argc, char** argv) {
    // 用法： peerTest            正常路径，6 个调用都应成功
    //        peerTest die <n>    失败路径：服务端处理 n 条后断开，
    //                            要求所有调用都被唤醒（不能有人永久挂起）
    const bool dieMode = (argc > 1 && std::string(argv[1]) == "die");
    if (dieMode) {
        g_die_after.store(argc > 2 ? std::atoi(argv[2]) : 2);
    }

    uint16_t port = 0;
    const int lfd = listenOn(&port);
    if (lfd < 0) {
        std::printf("listen failed\n");
        return 1;
    }

    std::thread srv(serverLoop, lfd, kClients);
    std::printf("%s: server on 127.0.0.1:%u, clients=%d, delay=%dms%s\n\n",
                dieMode ? "失败路径" : "正常路径", port, kClients, kServerDelayMs,
                dieMode ? ", 服务端中途断开" : "");

    m_sylar::IOManager iom("peer_test", 1);
    auto peer = std::make_shared<Peer>();
    peer->node_id = 1;

    bool connected = false;
    // 连接必须在 IOManager 线程内发起（m_sylar hook 需要调度器上下文）
    iom.schedule(m_sylar::TaskCoro20::create_coro(
        std::bind(connectPeer, peer, port, &connected)));

    ::usleep(300 * 1000);   // 等连接建好

    if (!connected) {
        std::printf("!! connect failed\n");
        std::_Exit(1);
    }
    std::printf("connected\n\n");

    // ★ 同时投递 N 个调用 —— 全部落在同一个 IOManager 线程、同一个 Peer 上
    for (int i = 0; i < kClients; ++i) {
        iom.schedule(m_sylar::TaskCoro20::create_coro(std::bind(oneCall, peer, i)));
    }

    // 等结果：非 die 模式下最多等 5s
    const int maxWait = dieMode ? 200 : 100;
    for (int waited = 0; waited < maxWait && (g_ok.load() + g_bad.load()) < kClients; ++waited) {
        ::usleep(50 * 1000);
    }

    const int done = g_ok.load() + g_bad.load();
    std::printf("\n结果: 完成=%d/%d  ok=%d bad=%d\n", done, kClients, g_ok.load(), g_bad.load());

    int code;
    if (dieMode) {
        // 失败路径的判据：所有人都被唤醒（没有挂死的），且没人在断开后还成功
        code = (done == kClients && g_bad.load() > 0) ? 0 : 1;
        std::printf("失败路径判据: 全部唤醒 = %s, 有失败 = %s\n", done == kClients ? "是" : "否(有挂死!)",
                    g_bad.load() > 0 ? "是" : "否");
    } else {
        code = (g_ok.load() == kClients) ? 0 : 1;
    }

    // ★ 这里刻意跳过正常析构，原因有两个 m_sylar 的坑：
    //   1) IOManager::getInstance() == dynamic_cast<IOManager*>(Scheduler::GetThis())，
    //      在【非 IOManager 线程】上返回 nullptr；
    //      而 Socket::close() 和 ~Socket() 都会调 IOManager::getInstance()->closeFd()，
    //      所以在主线程释放 Socket 必然空指针解引用。
    //   2) 服务端线程此时正阻塞在 read() 上，join 会挂住。
    //   —— 结论：Socket 的释放必须在 IOManager 线程内做。
    std::_Exit(code);
}
