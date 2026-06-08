#ifndef __M_CONSUMER_H__
#define __M_CONSUMER_H__

#include "../mqcommon/helper.hpp"
#include "../mqcommon/msg.pb.h"
#include <unordered_map>
#include <mutex>
#include <memory>
#include <assert.h>

namespace mq
{
    using ConsumerCallback = std::function<void(const std::string&, const BasecProperties *, const std::string&)>;//参数是消费者标识,信息属性,信息主体
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

    // 以队列为单位的消费者管理单元
    class QueueConsumer
    {
    public:
        using ptr = std::shared_ptr<QueueConsumer>;

        QueueConsumer(const std::string &qname) : _qname(qname), _rr_seq(0) {
        }

        Consumer::ptr create(const std::string &ctag, const std::string &queue_name, bool ack_flag, const ConsumerCallback &cb)
        {
            // 加锁
            std::unique_lock<std::mutex> lock(_mutex);
            // 判断消费者是否重复
            for (auto &consumer : _consumers)
            {
                if (consumer->tag == ctag)
                    return Consumer::ptr();
            }
            // 没有则构造消费者对象
            Consumer::ptr consumer = std::make_shared<Consumer>(ctag, queue_name, ack_flag, cb);
            // 添加管理对象后返回
            _consumers.push_back(consumer);
            return consumer;
        }

        void remove(const std::string &ctag)
        {
            // 加锁
            std::unique_lock<std::mutex> lock(_mutex);
            // 判断是否存在,存在则删除
            for (std::vector<Consumer::ptr>::iterator it = _consumers.begin(); it != _consumers.end(); ++it)
            {
                if ((*it)->tag == ctag)
                {
                    _consumers.erase(it);
                    return;
                }
            }
        }

        Consumer::ptr choose()
        {
            std::unique_lock<std::mutex> lock(_mutex);
            if (_consumers.size() == 0)
                return Consumer::ptr();
            int idx = _rr_seq % _consumers.size();
            _rr_seq++;
            return _consumers[idx];
        }

        bool empty()
        {
            std::unique_lock<std::mutex> lock(_mutex);
            return _consumers.empty();
        }
        bool exists(const std::string &ctag)
        {
            std::unique_lock<std::mutex> lock(_mutex);
            for (std::vector<Consumer::ptr>::iterator it = _consumers.begin(); it != _consumers.end(); ++it)
            {
                if ((*it)->tag == ctag)
                {
                    return true;
                }
            }
            return false;
        }
        void clear()
        {
            std::unique_lock<std::mutex> lock(_mutex);
            _consumers.clear();
            _rr_seq = 0;
        }

    private:
        std::string _qname;                    // 队列名称
        std::mutex _mutex;                     // 锁
        uint64_t _rr_seq;                      // 轮转序号
        std::vector<Consumer::ptr> _consumers; // 消费者管理的数据结构
    };

    //管理所有消费者的管理单元,以队列为单位进行管理
    class ConsumerManager
    {
    public:
        using ptr = std::shared_ptr<ConsumerManager>;

        ConsumerManager() {};
        // 初始化队列
        void initQueueConsumer(const std::string &queue_name)
        {
            // 加锁
            std::unique_lock<std::mutex> lock(_mutex);
            // 判断是否重复
            auto it = _qconsumers.find(queue_name);
            if (it != _qconsumers.end())
            {
                return;
            }
            // 创建对象并加入
            QueueConsumer::ptr qcptr = std::make_shared<QueueConsumer>(queue_name);
            _qconsumers.insert(std::make_pair(queue_name, qcptr));
        }

        // 销毁队列
        void destroyQueueConsumer(const std::string &queue_name)
        {
            std::unique_lock<std::mutex> lock(_mutex);
            _qconsumers.erase(queue_name);
        }

        // 创建消费者
        Consumer::ptr create(const std::string &ctag, const std::string &queue_name, bool ack_flag, const ConsumerCallback &cb)
        {
            QueueConsumer::ptr qcptr;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _qconsumers.find(queue_name);
                if (it == _qconsumers.end())
                {
                    DBG_LOG("创建消费者时,没有找到对应队列%s!!", queue_name.c_str());
                    return Consumer::ptr();
                }
                qcptr = it->second;
            }
            return qcptr->create(ctag, queue_name, ack_flag, cb);
        }

        // 删除消费者
        void remove(const std::string &ctag, const std::string &queue_name)
        {
            QueueConsumer::ptr qcptr;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _qconsumers.find(queue_name);
                if (it == _qconsumers.end())
                {
                    DBG_LOG("删除消费者时,没有找到对应队列%s!!", queue_name.c_str());
                }
                qcptr = it->second;
            }
            qcptr->remove(ctag);
        }

        // 获取指定消费者
        Consumer::ptr choose(const std::string &queue_name)
        {
            QueueConsumer::ptr qcptr;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _qconsumers.find(queue_name);
                if (it == _qconsumers.end())
                {
                    DBG_LOG("获取指定消费者时,没有找到对应队列%s!!", queue_name.c_str());
                    return Consumer::ptr();
                }
                qcptr = it->second;
            }
            return qcptr->choose();
        }

        bool empty(const std::string &queue_name)
        {
            QueueConsumer::ptr qcptr;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _qconsumers.find(queue_name);
                if (it == _qconsumers.end())
                {
                    DBG_LOG("获取指定消费者时,没有找到对应队列%s!!", queue_name.c_str());
                    return false;
                }
                qcptr = it->second;
            }
            return qcptr->empty();
        }

        bool exists(const std::string &ctag, const std::string &queue_name)
        {
            QueueConsumer::ptr qcptr;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _qconsumers.find(queue_name);
                if (it == _qconsumers.end())
                {
                    DBG_LOG("获取指定消费者时,没有找到对应队列%s!!", queue_name.c_str());
                    return false;
                }
                qcptr = it->second;
            }
            return qcptr->exists(ctag);
        }

        void clear()
        {
            std::unique_lock<std::mutex> lock(_mutex);
            _qconsumers.clear();
        }

    private:
        std::mutex _mutex;
        std::unordered_map<std::string, QueueConsumer::ptr> _qconsumers;
    };
}

#endif