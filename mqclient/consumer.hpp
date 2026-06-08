#ifndef __M_CONSUMER_H__
#define __M_CONSUMER_H__

#include "../mqcommon/helper.hpp"
#include "../mqcommon/msg.pb.h"
#include <unordered_map>
#include <memory>

namespace mq
{
    using ConsumerCallback = std::function<void(const std::string &, const BasecProperties *, const std::string &)>; // 参数是消费者标识,信息属性,信息主体
    struct Consumer
    {
        using ptr = std::shared_ptr<Consumer>;

        std::string tag;           // 消费者标识
        std::string qname;         // 消费者订阅的队列名称
        bool auto_ack;             // 是否自动应答
        ConsumerCallback callback; // 回调函数

        Consumer();
        Consumer(const std::string &ctag, const std::string &queue_name, bool ack_flag, const ConsumerCallback &cb)
            : tag(ctag), qname(queue_name), auto_ack(ack_flag), callback(cb) {}
    };
}

#endif