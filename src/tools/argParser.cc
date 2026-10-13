#include "tools/argParser.hpp"

#include <cstdio>
#include <utility>

namespace craft {

ArgParser::ArgParser(std::string progName) : m_progName(std::move(progName)) {
    if (m_progName.empty()) {
        m_progName = "program";
    }
}


void ArgParser::addOption(const std::string& shortName, const std::string& longName,
                          const std::string& help, Callback cb) {
    m_options.push_back(Option{shortName, longName, help, std::move(cb)});
}


const ArgParser::Option* ArgParser::find(const std::string& arg) const {
    for (const auto& opt : m_options) {
        if (opt.match(arg)) {
            return &opt;
        }
    }
    return nullptr;
}


bool ArgParser::splitLongEq(const std::string& arg, std::string& name, std::string& value) const {
    // 只认 "--xxx=yyy"，而且 xxx 必须是注册过的长名。
    // 不这么限一下的话，一个带 '=' 的普通值（比如路径里含 '='）会被误当成选项。
    if (arg.rfind("--", 0) != 0) {
        return false;
    }
    const size_t eq = arg.find('=');
    if (eq == std::string::npos) {
        return false;
    }
    const std::string candidate = arg.substr(0, eq);
    if (find(candidate) == nullptr) {
        return false;
    }
    name = candidate;
    value = arg.substr(eq + 1);
    return true;
}


ArgParser::Result ArgParser::parse(int argc, char** argv) {
    std::vector<std::string> args;
    if (argc > 1) {
        args.reserve(static_cast<size_t>(argc) - 1);
        for (int i = 1; i < argc; ++i) {        // 跳过 argv[0]（程序名）
            args.emplace_back(argv[i]);
        }
    }
    return parse(args);
}


ArgParser::Result ArgParser::parse(const std::vector<std::string>& args) {
    for (size_t i = 0; i < args.size(); ++i) {
        const std::string& arg = args[i];

        if (arg == "-h" || arg == "--help") {
            return Result::Help;
        }

        // "--long=value"
        std::string name;
        std::string value;
        if (splitLongEq(arg, name, value)) {
            if (!find(name)->cb(value)) {
                std::fprintf(stderr, "[args] 参数 %s 的值非法: %s\n", name.c_str(), value.c_str());
                return Result::Error;
            }
            continue;
        }

        const Option* opt = find(arg);
        if (opt == nullptr) {
            std::fprintf(stderr, "[args] 未知参数: %s\n", arg.c_str());
            return Result::Error;
        }
        if (i + 1 >= args.size()) {
            std::fprintf(stderr, "[args] 参数 %s 缺少值\n", arg.c_str());
            return Result::Error;
        }
        const std::string& v = args[++i];
        if (!opt->cb(v)) {
            std::fprintf(stderr, "[args] 参数 %s 的值非法: %s\n", arg.c_str(), v.c_str());
            return Result::Error;
        }
    }
    return Result::OK;
}


std::string ArgParser::usage() const {
    std::string out = "用法: " + m_progName + " [选项]\n\n选项:\n";

    auto appendLine = [&out](const std::string& names, const std::string& help) {
        out += "  " + names;
        constexpr size_t kNameWidth = 26;
        if (names.size() + 2 < kNameWidth) {
            out += std::string(kNameWidth - names.size() - 2, ' ');
        } else {
            out += "\n" + std::string(kNameWidth, ' ');
        }
        out += help + "\n";
    };

    for (const auto& opt : m_options) {
        std::string names = opt.shortName;
        if (!opt.longName.empty()) {
            names += (names.empty() ? "" : ", ") + opt.longName;
        }
        appendLine(names, opt.help);
    }
    appendLine("-h, --help", "显示本帮助");
    return out;
}

}  // namespace craft
