#ifndef __M_QUEUE_H__
#define __M_QUEUE_H__

#include "../mqcommon/helper.hpp"
#include "../mqcommon/msg.pb.h"
#include <google/protobuf/map.h>
#include <unordered_map>
#include <mutex>
#include <memory>
#include <assert.h>

namespace mq
{
    struct MsgQueue
    {
        using ptr = std::shared_ptr<MsgQueue>;
        std::string name;                                      // 信息队列的名字
        bool durable;                                          // 持久化标志
        bool exclusive;                                        // 是否独占
        bool auto_delete;                                      // 是否自动删除
        google::protobuf::Map<std::string, std::string> args; // 其他参数
        // std::unordered_map<std::string, std::string> args; // 其他参数

        // 构造函数
        MsgQueue() {};
        MsgQueue(const std::string qname,
                 bool qdurable,
                 bool qexclusive,
                 bool qauto_delete,
                 // const std::unordered_map<std::string, std::string>& qargs)
                 const google::protobuf::Map<std::string, std::string> &qargs) : name(qname),
                                                                                 durable(qdurable),
                                                                                 exclusive(qexclusive),
                                                                                 auto_delete(qauto_delete),
                                                                                 args(qargs)
        {
        }

        // 序列化args里的内容,返回一个字符串 key=val&key=val...
        std::string getArgs()
        {
            std::string result;
            for (auto pir : args)
            {
                result += pir.first + '=' + pir.second + '&';
            }
            return result;
        }

        // 反序列化字符串中的数据,将字符串的内容存储到args中
        void setArgs(const std::string &str_args)
        {
            std::vector<std::string> sub_args;
            SplitHelper::split(str_args, "&", sub_args);
            for (const std::string &str : sub_args)
            {
                size_t pos = str.find("=");
                std::string key = str.substr(0, pos);
                std::string val = str.substr(pos + 1);
                args[key] = val;
                // args.insert(std::make_pair(key, val));
            }
        }
    };

    using QueueMap = std::unordered_map<std::string, MsgQueue::ptr>;
    // 队列数据持久化管理类
    class MsgQueueMapper
    {
    public:
        // 构造函数
        MsgQueueMapper(const std::string &dbfile) : _sql_helper(dbfile)
        {
            std::string path = FileHelper::parentDirectory(dbfile);
            FileHelper::createDirectory(path);
            assert(_sql_helper.open());
            createTable();
        }

        // 创建表
        void createTable()
        {
            // create table if not exists queue_table(name varchar(32) primary key,durable int,exclusive int,auto_delete int,args varchar(128));
            std::stringstream sql;
            sql << "create table if not exists queue_table(";
            sql << "name varchar(32) primary key,";
            sql << "durable int,";
            sql << "exclusive int,";
            sql << "auto_delete int,";
            sql << "args varchar(128));";
            assert(_sql_helper.exec(sql.str(), nullptr, nullptr));
        }

        // 删除表
        void removeTable()
        {
            std::string sql = "drop table if exists queue_table;";
            assert(_sql_helper.exec(sql, nullptr, nullptr));
        }

        // 插入数据
        bool insert(const MsgQueue::ptr &queue)
        {
            // insert into queue_table values('queue1',true,false,false,"k1=v1&k2=v2&");
            std::stringstream sql;
            sql << "insert into queue_table values(";
            sql << "'" << queue->name << "',";
            sql << queue->durable << ",";
            sql << queue->exclusive << ",";
            sql << queue->auto_delete << ",";
            sql << "'" << queue->getArgs() << "');";
            return _sql_helper.exec(sql.str(), nullptr, nullptr);
        }

        // 删除数据
        bool remove(const std::string &name)
        {
            // delete from queue_table where name='queue1';
            std::stringstream sql;
            sql << "delete from queue_table where name='" << name << "';";
            return _sql_helper.exec(sql.str(), nullptr, nullptr);
        }

        // 获取所有数据
        QueueMap recovery()
        {
            QueueMap result;
            std::string sql = "select name,durable,exclusive,auto_delete,args from queue_table;";
            _sql_helper.exec(sql, selectCallback, &result);
            return result;
        }

    private:
        static int selectCallback(void *arg, int numcol, char **row, char **fields)
        {
            // 将存储数据的容器做类型转换
            QueueMap *result = (QueueMap *)arg;
            // 构造Exchange所需要的5个参数
            const std::string name = row[0];
            bool durable = static_cast<bool>(std::stoi(row[1]));
            bool exclusive = static_cast<bool>(std::stoi(row[2]));
            bool auto_delete = static_cast<bool>(std::stoi(row[3]));
            //std::unordered_map<std::string,std::string> args;
            google::protobuf::Map<std::string, std::string> args;
            // 构造MsgQueue对象,并交由智能指针管理
            MsgQueue::ptr exptr = std::make_shared<MsgQueue>(name, durable, exclusive, auto_delete, args);
            if (numcol == 5 && row[4] != nullptr)
                exptr->setArgs(row[4]);
            // 插入构造好的对象
            result->insert(std::make_pair(name, exptr));
            return 0;
        }

    private:
        SqliteHelper _sql_helper; // 数据库操作类
    };

    // 消息队列管理类
    class MsgQueueManager
    {
    public:
        using ptr = std::shared_ptr<MsgQueueManager>;
        MsgQueueManager(const std::string &dbfile) : _mapper(dbfile)
        {
            _msg_queues = _mapper.recovery();
            DBG_LOG("读取队列信息成功!");
        }

        // 声明队列
        bool declareQueue(const std::string qname,
                          bool qdurable,
                          bool qexclusive,
                          bool qauto_delete,
                          //const std::unordered_map<std::string, std::string>& qargs)
                          const google::protobuf::Map<std::string, std::string>& qargs)
        {
            std::unique_lock<std::mutex> lock(_mutex);
            QueueMap::iterator it = _msg_queues.find(qname);
            if (it != _msg_queues.end())
            {
                return true;
            }
            MsgQueue::ptr mqp = std::make_shared<MsgQueue>(qname, qdurable, qexclusive, qauto_delete, qargs);
            if (qdurable == true)
            {
                bool ret = _mapper.insert(mqp);
                if (ret == false)
                    return false;
            }
            _msg_queues.insert(make_pair(qname, mqp));
            return true;
        }

        // 删除队列
        bool deleteQueue(const std::string &name)
        {
            std::unique_lock<std::mutex> lock(_mutex);
            QueueMap::iterator it = _msg_queues.find(name);
            if (it == _msg_queues.end())
            {
                return true;
            }
            if (it->second->durable == true)
            {
                bool ret = _mapper.remove(name);
                if (ret == false)
                    return false;
            }
            _msg_queues.erase(name);
            return true;
        }

        // 查询指定队列
        MsgQueue::ptr selectQueue(const std::string &name)
        {
            std::unique_lock<std::mutex> lock(_mutex);
            QueueMap::iterator it = _msg_queues.find(name);
            if (it == _msg_queues.end())
            {
                return nullptr;
            }
            return it->second;
        }

        // 获取所有队列
        const QueueMap& allQueues()
        {
            std::unique_lock<std::mutex> lock(_mutex);
            return _msg_queues;
        }

        // 指定队列是否存在
        bool exists(const std::string &name)
        {
            std::unique_lock<std::mutex> lock(_mutex);
            QueueMap::iterator it = _msg_queues.find(name);
            if (it == _msg_queues.end())
            {
                return false;
            }
            return true;
        }

        // 获取队列个数
        size_t size()
        {
            std::unique_lock<std::mutex> lock(_mutex);
            return _msg_queues.size();
        }

        // 清理所有队列
        void clear()
        {
            std::unique_lock<std::mutex> lock(_mutex);
            _mapper.removeTable();
            _msg_queues.clear();
        }

    private:
        std::mutex _mutex;
        MsgQueueMapper _mapper; // 队列数据持久化管理类
        QueueMap _msg_queues;   // 队列列表
    };
}

#endif