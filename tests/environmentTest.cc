#include <iostream>
#include <sylar/basic/log.h>


int main() {
    m_sylar::Logger::ptr test_logger = M_SYLAR_LOG_NAME("test");
    M_SYLAR_LOG_INFO(test_logger) << "Normal environment.";
    return 0;
}