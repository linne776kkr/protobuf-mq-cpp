#ifndef __M_HOST_H__
#define __M_HOST_H__

#include "exchange.hpp"
#include "queue.hpp"
#include "binding.hpp"
#include "message.hpp"

namespace mq
{
    class VirtualHost
    {
    public:
        using ptr = std::shared_ptr<VirtualHost>;

        VirtualHost(const std::string &name, const std::string &dbfile, const std::string &basedir) :
         _name(name),
        _emptr(std::make_shared<ExchangeManager>(dbfile)),
        _qmptr(std::make_shared<MsgQueueManager>(dbfile)),
        _bmptr(std::make_shared<BindingManager>(dbfile)),
        _mmptr(std::make_shared<MessageManager>(basedir))
        {
            // 获取到所有的队列消息,通过队列名恢复历史消息数据
            QueueMap qm = _qmptr->allQueues();
            for (auto &it : qm)
            {
                _mmptr->initQueueMessage(it.first);
            }
        }

        // 交换机的声明与删除,判断是否存在(测试用)
        bool declareExchange(const std::string &name,
                             ExchangeType type,
                             DeliveryMode durable,
                             bool auto_delete,
                             const google::protobuf::Map<std::string, std::string>& args)
        {
            return _emptr->declareExchange(name, type, durable, auto_delete, args);
        }

        bool deleteExchange(const std::string &name)
        {
            _bmptr->removeExchangeBindings(name);
            return _emptr->deleteExchange(name);
        }

        Exchange::ptr selectExchange(const std::string& name){
            return _emptr->selectExchange(name);
        }

        bool existsExchange(const std::string &name){
            return _emptr->exists(name);
        }

        // 队列的声明与删除,判断是否存在(测试用),获取所有队列信息
        bool declareQueue(const std::string qname,
                          bool qdurable,
                          bool qexclusive,
                          bool qauto_delete,
                          //std::unordered_map<std::string, std::string> qargs
                          const google::protobuf::Map<std::string, std::string>& qargs)
        {
            // 声明队列时还要去初始化信息的队列信息
            _mmptr->initQueueMessage(qname);
            return _qmptr->declareQueue(qname, qdurable, qexclusive, qauto_delete, qargs);
        }

        bool deleteQueue(const std::string &name)
        {
            // 删除队列需要同时清理绑定信息,信息的队列信息
            _bmptr->removeMsgQueueBindings(name);
            _mmptr->destroyQueueMessage(name);
            return _qmptr->deleteQueue(name);
        }

        const QueueMap& allQueues(){
            return _qmptr->allQueues();
        }

        bool existsQueue(const std::string &name){
            return _qmptr->exists(name);
        }

        // 绑定的声明与删除,和获取交换机的所有绑定信息,判断是否存在(测试用)
        bool bind(const std::string &ename, const std::string &qname, const std::string &key)
        {
            Exchange::ptr eptr = _emptr->selectExchange(ename);
            if (eptr == nullptr)
            {
                DBG_LOG("进行绑定时交换机%s不存在!", ename.c_str());
                return false;
            }
            MsgQueue::ptr qptr = _qmptr->selectQueue(qname);
            if (qptr == nullptr)
            {
                DBG_LOG("进行绑定时队列%s不存在!", qname.c_str());
                return false;
            }
            return _bmptr->bind(ename, qname, key, eptr->durable && qptr->durable);
        }

        void unBind(const std::string &ename, const std::string &qname)
        {
            _bmptr->unBind(ename, qname);
        }

        MsgQueueBindingMap exchangeBindings(const std::string &ename)
        {
            return _bmptr->getExchangeBindings(ename);
        }

        bool existsBinding(const std::string &ename, const std::string &qname){
            return _bmptr->exists(ename,qname);
        }

        // 消息的发布,消费,和确认
        bool basicPublish(const std::string &qname, const BasecProperties *basecProperties, const std::string &body)
        {
            MsgQueue::ptr qptr = _qmptr->selectQueue(qname);
            if (qptr == nullptr)
            {
                DBG_LOG("进行绑定时队列%s不存在!", qname.c_str());
                return false;
            }
            return _mmptr->insert(qname, basecProperties, body, qptr->durable);
        }

        MessageMapper::MessagePtr basicConsume(const std::string &qname)
        {
            return _mmptr->front(qname);
        }

        void basicAck(const std::string &qname, const std::string &msg_id)
        {
            _mmptr->ack(qname, msg_id);
        }

        void clear(){
            _emptr->clear();
            _qmptr->clear();
            _bmptr->clear();
            _mmptr->clear();
        }

    private:
        std::string _name;
        ExchangeManager::ptr _emptr;
        MsgQueueManager::ptr _qmptr;
        BindingManager::ptr _bmptr;
        MessageManager::ptr _mmptr;
    };
}

#endif