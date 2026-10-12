#pragma once
#include <cstdint>
#include <vector>
#include <coroutine/coro20/task.hpp>

#include "rpc/rpcManager.hpp"
#include "types.pb.h"

namespace craft {

// 日志管理 ----------------------------------------
class NodeLog {
public:
    using ptr = std::shared_ptr<NodeLog>;
    [[nodiscard]] size_t getLastLogIdx() const { return m_last_log_idx; }
    [[nodiscard]] size_t getLastLogTerm() const { return m_last_log_term; }
private:
    size_t m_last_log_term {0};                                     // 最新未提交日志term
    size_t m_last_log_idx {0};                                        // 最新未提交日志编号
};




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

    NodeState() = default;
    ~NodeState() = default;

    void init(int id, size_t node_count);

    bool becomeFollower(uint64_t term);         // 任意
    bool becomeCandidate();                     // follower
    bool becomeLeader();                        // candidate

    bool gainVote(int id, size_t term);
    bool castVote(int id, size_t term, const NodeLog& log,size_t last_log_index, size_t last_log_term);

    [[nodiscard]] State getState() const {return m_node_state;}
    [[nodiscard]] int getId() const {return m_id;}
    [[nodiscard]] size_t getTerm() const { return m_term; }
    [[nodiscard]] int getVoteCount() const { return m_vote_count.load(); }

private:
    State m_node_state {State::InValid};                         // 节点核心身份
    int m_id {-1};
    int m_voted_for {-1};                            // 投票给的节点
    size_t m_term {0};                                          // 当前任期

    std::vector<size_t> m_votes {0};                             // 节点投票信息
    std::atomic<int> m_vote_count {1};                          // 当前节点收到的投票数, 更新任期后重置为1

};




// 节点总控 ----------------------------------------
class RaftNode : public std::enable_shared_from_this<RaftNode>{
public:

public:
    RaftNode();
    ~RaftNode();

    // 流程控制
    m_sylar::Task<void> init(craft::RPC::RpcDefine& define, int node_id);
    void start();
    m_sylar::Task<void> stop();

    // 选举函数
    // 此函数不管是不是有leader,在自己不是leader时直接就请求选举，因此需要其他位置确定状态
    m_sylar::Task<void, m_sylar::TaskBeginExecuter> startElection();            // 开始申请投票，
    m_sylar::Task<void> handleRequestVote(std::shared_ptr<RPC::Request> req, RPC::RPCSession::ptr session);            // 投票给其他人，未在term内投票且对方符合资质

    // log处理
    m_sylar::Task<void, m_sylar::TaskBeginExecuter> appendEntries();        // leadre心跳发送
    m_sylar::Task<void> handleAppendEntries(std::shared_ptr<RPC::Request> req, RPC::RPCSession::ptr session);            // 投票给其他人，未在term内投票且对方符合资质


    // 状态函数
    [[nodiscard]] NodeState::State getState() const {return m_state.getState();}
    [[nodiscard]] RPC::RPCClient::ptr getClient() const {return m_mgr->getClient();}

protected:
    // 心跳处理，由appendEntries路由到此
    m_sylar::Task<void> handleHeartBeat();

private:
    int m_id {-1};                                                              // 节点id
    NodeState m_state;
    NodeLog m_log;      // 日志记录
    size_t m_node_count {0};                                                    // 总节点数目
    std::atomic<int> m_rpc_state {0};           // -1 错误, 0 未初始化， 1 未开始， 2 已启动， 3 stop
    craft::RPCManager* m_mgr {nullptr};

    // 定时器设置
    uint64_t m_recent_leader_heart {0};
    uint64_t m_election_interval_time {0};
    m_sylar::TimeTask::ptr m_election_timer {nullptr};      // 选举定时器
    m_sylar::TimeTask::ptr m_leader_timer {nullptr};        // 成为leader后的周期定时器
};



}
