#ifndef __M_MESSAGE_H_
#define __M_MESSAGE_H_

#include "../mqcommon/helper.hpp"
#include "../mqcommon/msg.pb.h"
#include <unordered_map>
#include <mutex>
#include <memory>
#include <assert.h>
#include <list>

namespace mq
{
    class MessageMapper
    {
#define DATAFILE_SUBFIX ".mqd"
#define TMPFILE_SUBFIX ".mqd.tmp"
    public:
        using MessagePtr = std::shared_ptr<mq::Message>;
        MessageMapper(const std::string &qname, std::string basedir) : _qname(qname)
        {
            if (basedir.back() != '/')
                basedir.push_back('/');
            FileHelper::createDirectory(basedir);
            _datafile = basedir + _qname + DATAFILE_SUBFIX;
            _tmpfile = basedir + _qname + TMPFILE_SUBFIX;
            createMsgFile();
        }

        // 创建消息文件
        bool createMsgFile()
        {

            bool ret = FileHelper::createFile(_datafile);
            if (ret == false)
            {
                DBG_LOG("创建队列数据文件%s失败!", _datafile.c_str());
                return false;
            }
            return true;
        }
        // 删除消息文件
        void removeMsgFile()
        {
            FileHelper::removeFile(_datafile);
            FileHelper::removeFile(_tmpfile);
        }
        // 将消息写入到文件
        bool insert(MessagePtr &msgptr)
        {
            return insert(_datafile, msgptr);
        }
        // 删除队列消息文件的消息
        bool remove(MessagePtr &msgptr)
        {
            // 1.将msgptr中的有效标志位修改为"0"
            msgptr->mutable_payload()->set_valid("0");
            // 2.对msg进行序列化
            std::string body = msgptr->payload().SerializeAsString();
            if (body.size() != msgptr->length())
            {
                DBG_LOG("不能修改文件中的信息,序列化后数据与原数据长度不一致!");
                return false;
            }
            // 3.将序列化后的消息覆盖原消息
            FileHelper helper(_datafile);
            bool ret = helper.write(body.c_str(), msgptr->offset(), msgptr->length());
            if (ret == false)
            {
                DBG_LOG("向队列数据文件覆盖数据失败!");
                return false;
            }
            return true;
        }
        std::list<MessagePtr> gc()
        {
            std::list<MessagePtr> result;
            bool ret = load(result);
            DBG_LOG("恢复数据时读取到%d条数据",result.size());
            if (ret == false)
            {
                DBG_LOG("加载有效数据失败!");
                return result;
            }
            // 2.将有效数据序列化后存储到临时文件中
            FileHelper::createFile(_tmpfile);
            for (auto &msgptr : result)
            {
                ret = insert(_tmpfile, msgptr);
                if (ret == false)
                {
                    DBG_LOG("向临时文件写入消息数据失败!");
                    return result;
                }
            }
            // 3.删除源文件
            ret = FileHelper::removeFile(_datafile);
            if (ret == false)
            {
                DBG_LOG("删除源文件失败!");
                return result;
            }
            // 4.修改临时文件名为源文件
            ret = FileHelper(_tmpfile).rename(_datafile);
            if (ret == false)
            {
                DBG_LOG("修改临时文件名为源文件失败!");
                return result;
            }
            // 5.返回新的有效数据
            return result;
        }

    private:
        bool insert(std::string file, MessagePtr &msgptr)
        {
            // 1.进行消息的序列化,获取到格式化后的消息
            //std::string body = msgptr->payload().SerializeAsString();
            std::string body;
            msgptr->payload().SerializeToString(&body);
            // 2.获取写入位置和文件长度
            FileHelper helper(file);
            size_t offset = helper.size();
            size_t length = body.size();
            // 3.将数据写入文件指定位置
            bool ret = helper.write((char *)&length, offset, sizeof(length));
            if (ret == false)
            {
                DBG_LOG("向队列数据文件写入数据长度失败!");
                return false;
            }

            ret = helper.write(body.c_str(), offset + sizeof(length), length);
            if (ret == false)
            {
                DBG_LOG("向队列数据文件写入数据失败!");
                return false;
            }
            // 4.更新msg中的实际存储信息
            msgptr->set_offset(offset + sizeof(length));
            msgptr->set_length(length);
            return true;
        }
        bool load(std::list<MessagePtr>& result)
        {
            // 1.加载出文件中的所有有效数据,存储格式,4字节长度|数据|4字节长度|数据|...
            FileHelper helper(_datafile);
            size_t offset = 0;
            size_t length;
            while (offset < helper.size())
            {
                length = 0;
                bool ret = helper.read((char *)&length, offset, sizeof(size_t));
                if (ret == false)
                {
                    DBG_LOG("读取消息长度失败!");
                    return false;
                }

                offset += sizeof(size_t);
                std::string body(length, '\0');
                ret = helper.read(&body[0], offset, length);
                if (ret == false)
                {
                    DBG_LOG("读取消息数据失败!");
                    return false;
                }

                offset += length;
                // 处理无效消息
                MessagePtr msgptr = std::make_shared<Message>();
                //msgptr->mutable_payload().ParseFromString(body);
                msgptr->mutable_payload()->ParseFromString(body);
                if (msgptr->payload().valid() == "0")
                    continue;
                result.push_back(msgptr);
            }
            return true;
        }

    private:
        std::string _qname;    // 消息存储在的队列的名字
        std::string _datafile; // 存储消息的文件
        std::string _tmpfile;  // 存储消息的临时文件
    };

    class QueueMessage
    {
    public:
        using ptr = std::shared_ptr<QueueMessage>;
        QueueMessage(const std::string &qname, std::string basedir) : _qname(qname), _total_count(0), _mapper(qname, basedir) {}

        void recovery()
        {
            std::unique_lock<std::mutex> lock(_mutex);
            _msgs = _mapper.gc();
            for (auto &msgptr : _msgs)
            {
                _durable_msgs.insert(std::make_pair(msgptr->payload().properties().id(), msgptr));
            }
            _total_count = _durable_msgs.size();
        }

        bool insert(const BasecProperties *basecProperties, const std::string &body, bool queue_is_duralbe)
        {
            // 1.构造消息对象
            MessageMapper::MessagePtr msgptr = std::make_shared<Message>();
            if (basecProperties != nullptr)
            {
                DeliveryMode mode = queue_is_duralbe?basecProperties->deliver_mode():DeliveryMode::UNDURABLE;
                msgptr->mutable_payload()->mutable_properties()->set_id(basecProperties->id());
                msgptr->mutable_payload()->mutable_properties()->set_deliver_mode(mode);
                msgptr->mutable_payload()->mutable_properties()->set_roting_key(basecProperties->roting_key());
            }
            else
            {
                DeliveryMode mode = queue_is_duralbe?DeliveryMode::DURABLE:DeliveryMode::UNDURABLE;
                msgptr->mutable_payload()->mutable_properties()->set_id(UUIDHelper::uuid());
                msgptr->mutable_payload()->mutable_properties()->set_deliver_mode(mode);
                msgptr->mutable_payload()->mutable_properties()->set_roting_key("");
            }
            msgptr->mutable_payload()->set_body(body);
            // 2.判断消息是否需要持久化
            std::unique_lock<std::mutex> lock(_mutex);
            if (msgptr->payload().properties().deliver_mode() == DURABLE)
            {
                // 3.进行持久化存储
                msgptr->mutable_payload()->set_valid("1");
                bool ret = _mapper.insert(msgptr);
                if (ret == false)
                {
                    DBG_LOG("进行持久化存储失败%s", body.c_str());
                    return false;
                }
                _total_count++;
                _durable_msgs.insert(std::make_pair(msgptr->payload().properties().id(), msgptr));
            }
            // 4.更新内存的管理
            _msgs.push_back(msgptr);
            return true;
        }

        // 获取队首数据
        MessageMapper::MessagePtr front()
        {
            std::unique_lock<std::mutex> lock(_mutex);
            if (_msgs.size() == 0)
            {
                DBG_LOG("队列数据为空,获取队首%s数据失败!", _qname.c_str());
                return nullptr;
            }
            MessageMapper::MessagePtr msgptr = _msgs.front();
            _msgs.pop_front();
            _waitack_msgs.insert(std::make_pair(msgptr->payload().properties().id(), msgptr));
            return msgptr;
        }

        // 每次删除后判断是否需要垃圾回收
        void remove(const std::string &msg_id)
        {
            std::unique_lock<std::mutex> lock(_mutex);
            // 1.从待确认队列中查找消息
            auto it = _waitack_msgs.find(msg_id);
            if (it == _waitack_msgs.end())
            {
                DBG_LOG("没有找到要删除的信息!");
            }
            // 2.根据消息的持久化模式,决定是否删除持久化信息
            if (it->second->payload().properties().deliver_mode() == DURABLE)
            {
                // 3.删除持久化信息
                _mapper.remove(it->second);
                _durable_msgs.erase(msg_id);
                // 持久化的消息总量大于2000并且有效比例低于50%则需要垃圾清理
                if (_total_count > 2000 && _durable_msgs.size() * 10 / _total_count < 5)
                {
                    _mapper.gc();
                    _total_count = _durable_msgs.size();
                }
            }
            // 4.删除内存信息
            DBG_LOG("确认消息成功,删除了%s",it->second->payload().body().c_str());
            _waitack_msgs.erase(msg_id);
        }

        size_t push_count()
        {
            std::unique_lock<std::mutex> lock(_mutex);
            return _msgs.size();
        }

        size_t total_count()
        {
            std::unique_lock<std::mutex> lock(_mutex);
            return _total_count;
        }

        size_t durable_count()
        {
            std::unique_lock<std::mutex> lock(_mutex);
            return _durable_msgs.size();
        }

        size_t waitack_count()
        {
            std::unique_lock<std::mutex> lock(_mutex);
            return _waitack_msgs.size();
        }

        void clear()
        {
            std::unique_lock<std::mutex> lock(_mutex);
            _mapper.removeMsgFile();
            _msgs.clear();
            _durable_msgs.clear();
            _waitack_msgs.clear();
        }

    private:
        std::mutex _mutex;
        std::string _qname;
        size_t _total_count;                                                      // 总的持久化消息数量
        MessageMapper _mapper;                                                    // 持久化管理句柄
        std::list<MessageMapper::MessagePtr> _msgs;                               // 待推送消息
        std::unordered_map<std::string, MessageMapper::MessagePtr> _durable_msgs; // 持久化消息
        std::unordered_map<std::string, MessageMapper::MessagePtr> _waitack_msgs; // 待确认消息
    };

    class MessageManager
    {
    public:
        using ptr = std::shared_ptr<MessageManager>;
        MessageManager(const std::string &basedir) : _basedir(basedir) {}
        // 初始化队列信息
        void initQueueMessage(const std::string &qname)
        {
            QueueMessage::ptr qmptr;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _queue_msgs.find(qname);
                if (it != _queue_msgs.end())
                {
                    DBG_LOG("初始化队列信息失败,消息已存在!");
                    return;
                }
                qmptr = std::make_shared<QueueMessage>(qname, _basedir);
                _queue_msgs.insert(std::make_pair(qname, qmptr));
            }
            qmptr->recovery();
            DBG_LOG("队列%s恢复未确认信息成功!",qname.c_str());
        }
        // 删除队列信息
        void destroyQueueMessage(const std::string &qname)
        {
            QueueMessage::ptr qmptr;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _queue_msgs.find(qname);
                if (it == _queue_msgs.end())
                {
                    return;
                }
                qmptr = it->second;
                _queue_msgs.erase(qname);
            }
            qmptr->clear();
        }
        bool insert(const std::string &qname, const BasecProperties *basecProperties, const std::string &body, bool queue_is_durable)
        {
            QueueMessage::ptr qmptr;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _queue_msgs.find(qname);
                if (it == _queue_msgs.end())
                {
                    DBG_LOG("向队列中插入数据失败,没有找到队列%s", qname.c_str());
                    return false;
                }
                qmptr = it->second;
            }
            return qmptr->insert(basecProperties, body, queue_is_durable);
        }
        // 获取队首消息
        MessageMapper::MessagePtr front(const std::string &qname)
        {
            QueueMessage::ptr qmptr;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _queue_msgs.find(qname);
                if (it == _queue_msgs.end())
                {
                    DBG_LOG("获取队首消息失败,没有找到队列消息管理句柄%s", qname.c_str());
                    return nullptr;
                }
                qmptr = it->second;
            }
            return qmptr->front();
        }
        
        void ack(const std::string &qname, const std::string &msg_id)
        {
            QueueMessage::ptr qmptr;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _queue_msgs.find(qname);
                if (it == _queue_msgs.end())
                {
                    DBG_LOG("确认消息失败,没有找到队列消息管理句柄%s", qname.c_str());
                    return;
                }
                qmptr = it->second;
            }
            qmptr->remove(msg_id);
        }

        size_t push_count(const std::string &qname)
        {
            QueueMessage::ptr qmptr;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _queue_msgs.find(qname);
                if (it == _queue_msgs.end())
                {
                    DBG_LOG("获取待推送消息数量失败,没有找到队列消息管理句柄%s", qname.c_str());
                    return 0;
                }
                qmptr = it->second;
            }
            return qmptr->push_count();
        }
        size_t total_count(const std::string &qname)
        {
            QueueMessage::ptr qmptr;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _queue_msgs.find(qname);
                if (it == _queue_msgs.end())
                {
                    DBG_LOG("获取总持久化消息数量失败,没有找到队列消息管理句柄%s", qname.c_str());
                    return 0;
                }
                qmptr = it->second;
            }
            return qmptr->total_count();
        }
        size_t durable_count(const std::string &qname)
        {
            QueueMessage::ptr qmptr;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _queue_msgs.find(qname);
                if (it == _queue_msgs.end())
                {
                    DBG_LOG("获取持久化消息数量失败,没有找到队列消息管理句柄%s", qname.c_str());
                    return 0;
                }
                qmptr = it->second;
            }
            return qmptr->durable_count();
        }
        size_t waitack_count(const std::string &qname)
        {
            QueueMessage::ptr qmptr;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _queue_msgs.find(qname);
                if (it == _queue_msgs.end())
                {
                    DBG_LOG("获取待确认消息数量失败,没有找到队列消息管理句柄%s", qname.c_str());
                    return 0;
                }
                qmptr = it->second;
            }
            return qmptr->waitack_count();
        }
        void clear()
        {
            std::unique_lock<std::mutex> lock(_mutex);
            for (auto &it : _queue_msgs)
            {
                it.second->clear();
            }
            _queue_msgs.clear();
        }

    private:
        std::mutex _mutex;
        std::string _basedir;
        std::unordered_map<std::string, QueueMessage::ptr> _queue_msgs;
    };
}

#endif