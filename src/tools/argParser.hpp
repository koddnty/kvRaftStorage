#pragma once

#include <functional>
#include <string>
#include <vector>

namespace craft {

/**
 * @brief 命令行参数解析器
 *
 * 先注册选项和回调，再把 main 的 argc/argv 丢给 parse()，
 * parse() 负责匹配、取值、调回调：
 *
 *     int selfId = -1;
 *     std::string conf = m_sylar::getExecutableDir() + "/conf/rpc.json";
 *
 *     craft::ArgParser parser("kvRaft");
 *     parser.addOption("-i", "--node-id", "本节点 id",
 *                      [&](const std::string& v) { selfId = std::atoi(v.c_str()); return true; });
 *     parser.addOption("-c", "--conf", "配置文件路径",
 *                      [&](const std::string& v) { conf = v; return true; });
 *
 *     switch (parser.parse(argc, argv)) {
 *         case craft::ArgParser::Result::OK:   break;
 *         case craft::ArgParser::Result::Help: std::printf("%s", parser.usage().c_str()); return 0;
 *         case craft::ArgParser::Result::Error:
 *             std::printf("%s", parser.usage().c_str());
 *             return 2;
 *     }
 *
 * 设计上只管「匹配 → 取值 → 调回调」，不关心每个选项的语义：
 *   · 回调只在参数【出现】时被调用；没出现就不调。所以「到底传没传」由调用方自己
 *     用变量的初值判断（上面 selfId 初值 -1 就是干这个的）。
 *   · 值合不合法由回调决定：返回 false 就整体失败，错误信息由 parser 打。
 *   · 值一律取下一个 token（和 getopt 一致）—— 即使它以 '-' 开头也照取，
 *     免得 "-i -1" 被误判成「缺少值」；合不合法交给回调。
 */
class ArgParser {
public:
    enum class Result {
        OK,         // 全部解析成功
        Help,       // 出现了 -h / --help，调用方应该打 usage 然后【正常】退出
        Error,      // 未知参数 / 缺值 / 回调返回 false，调用方应该打 usage 然后非 0 退出
    };

    /// 回调返回 false 表示这个值非法
    using Callback = std::function<bool(const std::string& value)>;

    explicit ArgParser(std::string progName = "program");

    /**
     * @param shortName 短名，形如 "-i"；不需要就给空串
     * @param longName  长名，形如 "--node-id"；不需要就给空串
     * @param help      usage 里的说明
     * @param cb        该参数出现时调用，返回值决定这个值是否合法
     */
    void addOption(const std::string& shortName, const std::string& longName,
                   const std::string& help, Callback cb);

    /// 给 main 用：跳过 argv[0]，其余交给下面那个重载
    Result parse(int argc, char** argv);
    /// 直接喂 token 列表（argv[0] 不包含在内），测试用这个
    Result parse(const std::vector<std::string>& args);

    std::string usage() const;

private:
    struct Option {
        std::string shortName;
        std::string longName;
        std::string help;
        Callback cb;

        bool match(const std::string& arg) const {
            return (!shortName.empty() && arg == shortName) ||
                   (!longName.empty() && arg == longName);
        }
    };

    const Option* find(const std::string& arg) const;
    /// 处理 "--long=value" 形式；命中（且长名注册过）返回 true 并填 name/value
    bool splitLongEq(const std::string& arg, std::string& name, std::string& value) const;

    std::string m_progName;
    std::vector<Option> m_options;
};

}  // namespace craft
