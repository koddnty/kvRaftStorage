#pragma once
#include <cstdint>
#include <vector>
#include <coroutine/coro20/task.hpp>

#include "rpc/rpcManager.hpp"
#include "log.hpp"          // dfix: NodeLog 定义在 log.hpp；不 include 的话 NodeState/RaftNode 里用不了它
#include "types.pb.h"

namespace craft {




// 节点状态 ----------------------------------------
class NodeState {
public:
    using ptr = std::shared_ptr<NodeState>;
    enum class State {
        InValid,        // 不可用状态
        Follower,
        Candidate,
        Leader
    };

    enum class VoteState {
        RETRY,
        SUCCESS,
        FAILED
    };

    NodeState() = default;
    ~NodeState() = default;

    void init(int id, size_t node_count);

    bool becomeFollower(uint64_t term);         // 任意
    bool becomeCandidate();                     // follower
    bool becomeLeader();                        // candidate

    VoteState gainVote(int id, size_t term); // 从id节点获得了投票
    VoteState castVote(int id, size_t term, const NodeLog& log,size_t last_log_index, size_t last_log_term); // 给身在term的节点一票

    void setTerm(const size_t term) {m_term = term;}

    [[nodiscard]] State getState() const {return m_node_state;}
    [[nodiscard]] int getId() const {return m_id;}
    [[nodiscard]] size_t getTerm() const { return m_term; }
    [[nodiscard]] int getVoteCount() const { return m_vote_count.load(); }
    [[nodiscard]] int getNodeCount() const {return m_votes.size(); }

private:
    State m_node_state {State::InValid};                         // 节点核心身份
    int m_id {-1};
    int m_voted_for {-1};                            // 投票给的节点
    size_t m_term {0};                                          // 当前任期

    std::vector<size_t> m_votes {0};                             // 节点投票信息
    std::atomic<int> m_vote_count {1};                          // 当前节点收到的投票数, 更新任期后重置为1

};


struct PeerState {
public:
    enum class State {
        Ready,      // 就绪
        Init        // 初始化中
    };
    uint64_t last_log_index = 0;
    uint64_t commit_log_index = 0;
    State state {State::Init};
};



// 节点总控 ----------------------------------------
class RaftNode : public std::enable_shared_from_this<RaftNode>{
public:
    RaftNode();
    ~RaftNode();

    enum class RespCode {
        SUCCESS,            // 成功
        InvalidState,       // 状态错误：如非leader节点为发送了复制日志请求
        MisMatchLog,        // 日志不匹配
        TimedOut,           // 回复超时
        RPCFailed,          // 通信错误
        PersistError,       // 持久化错误
        CommitError,        // 提交错误
        Other               // 其他
    };



    // 流程控制，函数没有经过锁保护，请调用时保证
    /**
     * @brief 初始化节点数据,初始化节点选举定时器和leader定时器,初始化rpc方法
     */
    m_sylar::Task<void> init(craft::RPC::RpcDefine& define, int node_id, const std::string& log_path);
    void start();       // 启动rpc(若未启动则无法完成其他通信依赖操作)
    m_sylar::Task<void> stop();

    // 添加操作
    m_sylar::Task<RespCode> push(const std::string& cmd);     // 发送命令


    // 选举函数
    /*
     *  选举出的leader资质：拥有最新的大部分节点已复制的日志。2
     */
    // 此函数不管是不是有leader,在自己不是leader时直接就请求选举，因此需要其他位置确定状态
    m_sylar::Task<void, m_sylar::TaskBeginExecuter> startElection();            // 开始申请投票，
    m_sylar::Task<void> handleRequestVote(std::shared_ptr<RPC::Request> req, RPC::RPCSession::ptr session);            // 投票给其他人，未在term内投票且对方符合资质

    // log处理
    /*
     * log 处理流程：leader向其他节点发送日志复制请求->leader收到大部分节点的success响应->
     *              leader本地commit日志，推动commitIdx->leader通知其他节点commit当前日志。
     */

    /**
     * @brief 作为leader向其他节点发送日志复制请求，如果日志前缀不匹配，会自动复制历史日志，但会拖慢当前日志复制请求速度
     */
    m_sylar::Task<RespCode> appendEntries(uint64_t idx);        // 日志复制
    m_sylar::Task<void, m_sylar::TaskBeginExecuter> appendDetectEntries();        // 探测空包（如心跳包）
    /**
     * @brief 函数执行无须加锁，TODO 线程安全。
     * @param detection 默认false，若为true会发送空包检测（不带日志项）
     */
    m_sylar::Task<RespCode> appendEntriesTo(uint64_t idx, size_t node_id, bool detection = false);          // 为节点i同步日志        // leadre心跳发送
    m_sylar::Task<void> handleAppendEntries(std::shared_ptr<RPC::Request> req, RPC::RPCSession::ptr session);            // 投票给其他人，未在term内投票且对方符合资质




    // 状态函数
    [[nodiscard]] NodeState::State getState() const {return m_state.getState();}
    [[nodiscard]] RPC::RPCClient::ptr getClient() const {return m_mgr->getClient();}

protected:
    // 心跳处理，由appendEntries路由到此
    m_sylar::Task<void, m_sylar::TaskBeginExecuter> heartBeat();
    m_sylar::Task<AppendEntriesReply> handleHeartBeat(const AppendEntriesArgs& entry);

protected:
    m_sylar::Task<bool>  becomeFollower(uint64_t term);
    m_sylar::Task<bool>  becomeCandidate();
    m_sylar::Task<bool>  becomeLeader();


private:
    m_sylar::CoMutex m_mutex;       // 资源锁


    int m_id {-1};                                                              // 节点id
    NodeState m_state;
    NodeLog m_log;      // 日志记录
    size_t m_node_count {0};                                                    // 总节点数目
    std::atomic<int> m_rpc_state {0};           // -1 错误, 0 未初始化， 1 未开始， 2 已启动， 3 stop
    craft::RPCManager* m_mgr {nullptr};
    std::vector<PeerState> m_peers_state;      // 对端节点状态

    // 定时器设置
    uint64_t m_election_interval_time {0};
    m_sylar::TimeTask::ptr m_election_timer {nullptr};      // 选举定时器
    m_sylar::TimeTask::ptr m_leader_timer {nullptr};        // 成为leader后的周期定时器
    std::atomic<uint64_t> m_recent_leader_heart {0};
};



}
