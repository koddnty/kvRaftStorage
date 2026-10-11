#include "node.hpp"



namespace craft {


static auto g_logger = M_SYLAR_LOG_NAME("craft");


// 节点状态 ----------------------------------------
void NodeState::becomeFollower(uint64_t term) {

}


void NodeState::becomeCandidate() {

}


void NodeState::becomeLeader() {

}




// 节点总控 ----------------------------------------
RaftNode::RaftNode() {

}


RaftNode::~RaftNode() {

}


// 流程控制
m_sylar::Task<void> RaftNode::init(craft::RPC::RpcDefine& define, int node_id) {
    if (rpc_state_ != 0) {
        M_SYLAR_LOG_ERROR(g_logger) << "bad raft node state: from unexpected(uninit) to start";
    }
    // 初始化manager
    id_ = node_id;
    mgr_ = craft::RPCManager::GetInstanceWithOutInit();
    mgr_->create();

    // 初始化client,server
    const int peers = co_await mgr_->getClient()->init(define, id_);
    int rt = co_await mgr_->getServer()->init(define, id_);
    if (rt == -1) {
        M_SYLAR_LOG_ERROR(g_logger) << " failed to init server";
        rpc_state_ = -1;
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

    // 其他变量设置
    rpc_state_ = 1;
    node_count_ = define.nodes.size();
    co_return;
}


void RaftNode::start() {
    if (rpc_state_ != 1) {
        M_SYLAR_LOG_ERROR(g_logger) << "bad raft node state: from unexpected(unstart) to start";
    }
    rpc_state_ = 2;
}


m_sylar::Task<void> RaftNode::stop() {
    rpc_state_ = 3;
    co_return;
}


// 选举函数,选举超时后使用
m_sylar::Task<void> RaftNode::startElection() {
    if (rpc_state_ == 0) {
        M_SYLAR_LOG_ERROR(g_logger) << "bad raft node state: not started";
        co_return;
    }
    // 成为candidatte
    state_.becomeCandidate();


    // 轮训所有节点, 发送成为leader的请求.
    // 装配请求
    auto req_bytes = std::make_shared<std::string>();
    craft::RequestVoteArgs req_vote;
    req_vote.set_term(1);                      // 任期
    req_vote.set_candidate_id(id_);                 // 节点id
    req_vote.set_last_log_index(1);
    req_vote.set_last_log_term(1);
    *req_bytes = req_vote.SerializeAsString();

    auto resp_bytes = std::make_shared<std::string>();
    // TODO 改为并行发送请求
    for (int i = 0; i < node_count_; i++) {
        if (i == id_) {continue; }

        RPC::CallState st = co_await mgr_->getClient()->call(i, "craft", "requestVote", req_bytes, resp_bytes);
        if (st != RPC::CallState::SUCCESS) {
            M_SYLAR_LOG_WARN(g_logger) << "failed to request vote to node {" << i << "}";
        }
        M_SYLAR_LOG_INFO(g_logger) << "I got a vote from " << i;

        // 响应 check TODO
        vote_count_++;
    }

    if (vote_count_ * 2 >= node_count_) {
        // 取得半数投票 TODO
        M_SYLAR_LOG_INFO(g_logger) << "成为leader";

        state_.becomeLeader();
    }
    vote_count_ = 1;
    co_return;
}


m_sylar::Task<void>
    RaftNode::handleRequestVote(std::shared_ptr<RPC::Request> req, RPC::RPCSession::ptr session) {

    // 解析请求
    RequestVoteArgs req_vote;
    req_vote.ParseFromString(req->data());

    // 直接给了 TODO
    RequestVoteReply reply;
    reply.set_term(req_vote.term());
    reply.set_vote_granted(true);


    RPC::Response resp;
    resp.set_id(req->id());
    resp.set_code(static_cast<int>(RPC::RpcCode::OK));
    resp.set_data(reply.SerializeAsString());

    RPC::Frame frame;
    frame.setData(resp.SerializeAsString());

    // dfix: co_sendMessage 返回 Task<void>，必须 co_await —— 否则发没发出去、发成功没有都不知道。
    M_SYLAR_LOG_INFO(g_logger) <<  "回复选举响应中...";

    m_sylar::IOState st = co_await session->co_sendMessage(frame);
    if (st != m_sylar::IOState::SUCCESS) {
        M_SYLAR_LOG_WARN(g_logger) <<  "选举响应失败";
    }
    co_return ;
}
}
