#include "node.hpp"
#include <functional>

#include <random>


namespace craft {


static auto g_logger = M_SYLAR_LOG_NAME("craft");

static inline m_sylar::ConfigVar<size_t>::ptr g_followerElectionTimeMin =
    m_sylar::ConfigManager::LookUp<size_t>
    ("kRaftTimer.followerElectionTimeMin", 400, kRpcConfId,
                                           "最短选举超时定时器时间ms");
static inline m_sylar::ConfigVar<size_t>::ptr g_followerElectionTimeMax =
    m_sylar::ConfigManager::LookUp<size_t>
    ("kRaftTimer.followerElectionTimeMax", 650, kRpcConfId,
                                           "最长选举超时定时器时间ms");
static inline m_sylar::ConfigVar<size_t>::ptr g_leaderHeartBeatTime =
    m_sylar::ConfigManager::LookUp<size_t>
    ("kRaftTimer.leaderHeartBeatTime", 150, kRpcConfId,
                                           "leader心跳循环时间ms");

static uint64_t getRandFollowerElectionTimeMs() {
    static std::mt19937 m_rng(std::random_device{}());
    static std::uniform_int_distribution<uint64_t> m_election_dist(g_followerElectionTimeMin->getValue(),
                                                                   g_followerElectionTimeMax->getValue());
    return m_election_dist(m_rng);
}



// 节点状态 ----------------------------------------
void NodeState::init(int id, size_t node_count) {
    m_id = id;
    m_votes.resize(node_count, 0);
}

bool NodeState::becomeFollower(uint64_t term) {
    M_SYLAR_LOG_INFO(g_logger) << "节点 " << m_id << " 成为follower";
    return true;
}


bool NodeState::becomeCandidate() {
    M_SYLAR_LOG_INFO(g_logger) << "节点 " << m_id << " 成为candidate";



    m_node_state = State::Candidate;
    m_voted_for = m_id;

    m_term++;
    // 重置投票列表
    for (unsigned long & m_vote : m_votes) {
        m_vote = 0;
    }
    m_votes[m_id] = 1;
    m_vote_count = 1;
    return true;
}


bool NodeState::becomeLeader() {
    M_SYLAR_LOG_INFO(g_logger) << "节点 " << m_id << " 成为leader";
    if (m_node_state != State::Candidate) {
        M_SYLAR_LOG_WARN(g_logger) << "无效状态：无法从非candidate状态转换到leader状态，当前状态：" << static_cast<int>(m_node_state);
        return false;
    }

    m_node_state = State::Leader;
    m_voted_for = m_id;

    return true;
}


// 从id节点获得了一票
bool NodeState::gainVote(int id, size_t term) {
    if (term != m_term || m_node_state != State::Candidate) {
        return false;
    }

    if (id < 0 || static_cast<size_t>(id) >= m_votes.size()) {
        return false;
    }

    if (m_votes[id] != 0) {
        return false;
    }

    m_votes[id] = 1;
    ++m_vote_count;
    if (m_vote_count * 2 > m_votes.size()) {
        return becomeLeader();      // 直接成为leader
    }
    return true;
}

// 给身在term的节点一票
bool NodeState::castVote(int id, size_t term, const NodeLog& log, size_t last_log_index, size_t last_log_term) {
    if (term < m_term) {
        return false;
    }

    if (term > m_term) {
        m_term = term;
        m_node_state = State::Follower;
        m_voted_for = -1;
    }

    if (m_voted_for != -1 && m_voted_for != id) {
        return false;
    }

    if (last_log_term < log.getLastLogTerm()) {
        return false;
    }

    if (last_log_term == log.getLastLogTerm() &&
        last_log_index < log.getLastLogIdx()) {
        return false;
        }

    m_voted_for = id;
    return true;
}


// 节点总控 ----------------------------------------
RaftNode::RaftNode() {
}


RaftNode::~RaftNode() {

}


// 流程控制
m_sylar::Task<void> RaftNode::init(craft::RPC::RpcDefine& define, int node_id) {
    if (m_rpc_state != 0) {
        M_SYLAR_LOG_ERROR(g_logger) << "bad raft node state: from unexpected(uninit) to start";
    }

    // 初始化节点状态信息
    m_node_count = define.nodes.size();
    m_state.init(node_id, m_node_count);

    // 初始化manager
    m_id = node_id;
    m_mgr = craft::RPCManager::GetInstanceWithOutInit();
    m_mgr->create();

    // 初始化client,server
    const int peers = co_await m_mgr->getClient()->init(define, m_id);
    int rt = co_await m_mgr->getServer()->init(define, m_id);
    if (rt == -1) {
        M_SYLAR_LOG_ERROR(g_logger) << " failed to init server";
        m_rpc_state = -1;
        co_return;
    }


    // 路由注册
    // 投票请求
    RPCManager::GetInstanceWithOutInit()->getServer()->
    registeRoute("craft", "requestVote",std::bind(
            &RaftNode::handleRequestVote,
            this,
            std::placeholders::_1,
            std::placeholders::_2
    ));


    // 初始化定时器
    // 配置节点定时器时间
    m_recent_leader_heart = m_sylar::GetCurrentTime_ms();
    m_election_interval_time = getRandFollowerElectionTimeMs();
    // 选举定时器配置
    if (m_election_timer != nullptr) { m_election_timer->cancel(); m_election_timer = nullptr; }
    m_election_timer = m_sylar::TimeTask::create(m_election_interval_time, true,
        [node = shared_from_this()](m_sylar::TimeTask::ptr task) -> m_sylar::Task<void> {
            // 检查leader心跳
            uint64_t curr_time = m_sylar::GetCurrentTime_ms();
            if (curr_time - node->m_recent_leader_heart > node->m_election_interval_time) {
                // 选举定时器超时， 调度选举处理函数
                m_sylar::IOManager::getInstance()->schedule(m_sylar::TaskCoro20::create_coro(
                    std::bind(&RaftNode::startElection, node)));
            }
            co_return;
        }
    );
    m_sylar::TimeManager::getInstance()->addTimer(m_election_timer);

    // leader心跳定时器配置
    if (m_leader_timer != nullptr) { m_leader_timer->cancel(); m_leader_timer = nullptr; }
    m_leader_timer = m_sylar::TimeTask::create(g_leaderHeartBeatTime->getValue(), true,
        [node = shared_from_this()](m_sylar::TimeTask::ptr task) -> m_sylar::Task<void> {
            // TODO 调度心跳处理函数
            M_SYLAR_LOG_INFO(g_logger) << "leader 调度了心跳处理函数";
            // node->getClient()->call("craft", "appendEntries",   );
            co_return;
        }
    );


    // 其他变量设置
    m_rpc_state = 1;
    co_return;
}


void RaftNode::start() {
    if (m_rpc_state != 1) {
        M_SYLAR_LOG_ERROR(g_logger) << "bad raft node state: from unexpected(unstart) to start";
    }
    m_rpc_state = 2;
}


m_sylar::Task<void> RaftNode::stop() {
    m_rpc_state = 3;
    co_return;
}


// 选举函数,选举超时后使用
m_sylar::Task<void, m_sylar::TaskBeginExecuter> RaftNode::startElection() {
    if (m_rpc_state == 0) {
        M_SYLAR_LOG_ERROR(g_logger) << "bad raft node state: not started";
        co_return;
    }
    // 成为candidatte
    m_state.becomeCandidate();


    // 轮训所有节点, 发送成为leader的请求.
    // 装配请求
    auto req_bytes = std::make_shared<std::string>();
    craft::RequestVoteArgs req_vote;
    req_vote.set_term(m_state.getTerm());                      // 任期
    req_vote.set_candidate_id(m_state.getId());                 // 节点id
    req_vote.set_last_log_index(m_log.getLastLogIdx());
    *req_bytes = req_vote.SerializeAsString();

    auto resp_bytes = std::make_shared<std::string>();
    // TODO 改为并行发送请求
    for (int i = 0; i < m_node_count; i++) {
        if (i == m_id) {continue; }

        RPC::CallState st = co_await m_mgr->getClient()->call(i, "craft", "requestVote", req_bytes, resp_bytes);
        if (st != RPC::CallState::SUCCESS) {
            M_SYLAR_LOG_WARN(g_logger) << "failed to request vote to node {" << i << "}";
            continue;
        }

        // resp 解析
        RequestVoteReply reply;
        reply.ParseFromString(*resp_bytes);
        if (reply.vote_granted() == true) {
            if (m_state.gainVote(i, reply.term())) {
                // 从i中获得了一个投票
                M_SYLAR_LOG_DEBUG(g_logger) << "从节点 " << i << " 中获得一个投票，当前状态：term "
                    << m_state.getTerm() << " votes: " << m_state.getVoteCount() << "/" << m_node_count;
            }
            else {
                M_SYLAR_LOG_INFO(g_logger) << "未能从节点 " << i << " 中获得投票";
            }

        }

        // 停止检查
        if (m_state.getState() == NodeState::State::Leader) {
            break;
        }
    }
    co_return;
}


m_sylar::Task<void> RaftNode::handleRequestVote
        (std::shared_ptr<RPC::Request> req, RPC::RPCSession::ptr session) {
    // 解析请求
    RequestVoteArgs req_vote;
    req_vote.ParseFromString(req->data());


    // 更新状态并设置响应内容
    RequestVoteReply reply;
    reply.set_vote_granted(
        m_state.castVote(req_vote.candidate_id(), req_vote.term(),
            m_log,
            req_vote.last_log_index(), req_vote.last_log_term()));
    reply.set_term(req_vote.term());


    RPC::Response resp;
    resp.set_id(req->id());
    resp.set_code(static_cast<int>(RPC::RpcCode::OK));
    resp.set_data(reply.SerializeAsString());

    m_sylar::IOState st = co_await session->co_sendRpcMessage(resp);
    if (st != m_sylar::IOState::SUCCESS) {
        M_SYLAR_LOG_WARN(g_logger) <<  "选举响应失败";
    }
    co_return ;
}


m_sylar::Task<void, m_sylar::TaskBeginExecuter> RaftNode::appendEntries() {
    co_return;
}


m_sylar::Task<void> RaftNode::handleAppendEntries
            (std::shared_ptr<RPC::Request> req, RPC::RPCSession::ptr session) {
    co_return;
}


m_sylar::Task<void> RaftNode::handleHeartBeat() {
    co_return;
}


}
