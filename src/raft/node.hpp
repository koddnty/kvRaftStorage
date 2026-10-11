#pragma once
#include <cstdint>
#include <vector>
#include <coroutine/coro20/task.hpp>

#include "rpc/rpcManager.hpp"
#include "types.pb.h"

namespace craft {

// 节点状态 ----------------------------------------
class NodeState {
public:
    enum class State {
        InValid,        // 不可用状态
        Follower,
        Candidate,
        Leader
    };

    void becomeFollower(uint64_t term);         // 任意
    void becomeCandidate();                     // follower
    void becomeLeader();                        // candidate

    [[nodiscard]] State getState() const {return node_state_;}
private:
    State node_state_ {State::InValid};         // 节点核心身份

};

// 节点总控 ----------------------------------------
class RaftNode {
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
    m_sylar::Task<void> startElection();            // 开始申请投票，
    m_sylar::Task<void> handleRequestVote(std::shared_ptr<RPC::Request> req, RPC::RPCSession::ptr session);            // 投票给其他人，未在term内投票且对方符合资质


    // 状态函数
    [[nodiscard]] NodeState::State getState() const {return state_.getState();}

private:
    int id_ {-1};                                                        // 节点id
    size_t node_count_ {0};                                                    // 总节点数目
    NodeState state_;
    std::atomic<int> vote_count_ {1};                          // 当前节点收到的投票数
    std::atomic<int> rpc_state_ {0};                             // -1 错误, 0 未初始化， 1 未开始， 2 已启动， 3 stop
    craft::RPCManager* mgr_ {nullptr};
};



}
