#ifndef __M_EXCHANGE_H__
#define __M_EXCHANGE_H__

#include "../mqcommon/helper.hpp"
#include "../mqcommon/msg.pb.h"
#include <google/protobuf/map.h>
#include <unordered_map>
#include <mutex>
#include <memory>
#include <assert.h>

namespace mq
{
    // 交换机类
    struct Exchange
    {
        using ptr = std::shared_ptr<Exchange>;
        std::string name;                                     // 交换机名称
        ExchangeType type;                                    // 交换机类型
        DeliveryMode durable;                                 // 持久化标志
        bool auto_delete;                                     // 是否自动删除
        google::protobuf::Map<std::string, std::string> args; // 其他参数
        // std::unordered_map<std::string, std::string> args;
        // 构造函数
        Exchange(){}
        Exchange(const std::string &ename,
                 ExchangeType etype,
                 DeliveryMode edurable,
                 bool eauto_delete,
                 const google::protobuf::Map<std::string, std::string> &eargs) : name(ename),
                                                                                 type(etype),
                                                                                 durable(edurable),
                                                                                 auto_delete(eauto_delete),
                                                                                 args(eargs)
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

    using ExchangeMap = std::unordered_map<std::string, Exchange::ptr>;
    // 交换机数据持久化管理类
    class ExchangeMapper
    {
    public:
        // 构造函数
        ExchangeMapper(const std::string &dbfile) : _sql_helper(dbfile)
        {
            std::string path = FileHelper::parentDirectory(dbfile);
            FileHelper::createDirectory(path);
            assert(_sql_helper.open());
            createTable();
        }

        // 创建表
        void createTable()
        {
#define CREATE_TABLE "create table if not exists exchange_table(\
                    name varchar(32) primary key,\
                    type int,\
                    durable int,\
                    auto_delete int,\
                    args varchar(128));"
            bool ret = _sql_helper.exec(CREATE_TABLE, nullptr, nullptr);
            if (ret == false)
            {
                ERR_LOG("创建交换机数据表失败!");
                abort(); // 程序异常退出
            }
        }

        // 删除表
        void removeTable()
        {
#define DROP_TABLE "drop table if exists exchange_table;"
            bool ret = _sql_helper.exec(DROP_TABLE, nullptr, nullptr);
            if (ret == false)
            {
                ERR_LOG("删除交换机数据表失败!");
                abort(); // 程序异常退出
            }
        }

        // 插入交换机
        bool insert(const Exchange::ptr &exptr)
        {
            // insert into exchange_table values('name',type,durable,auto_delete,'args');
            std::stringstream ss;
            ss << "insert into exchange_table values(";
            ss << "'" << exptr->name << "',";
            ss << exptr->type << ",";
            ss << exptr->durable << ",";
            ss << exptr->auto_delete << ",";
            ss << "'" << exptr->getArgs() << "');";
            return _sql_helper.exec(ss.str(), nullptr, nullptr);
        }

        // 删除指定交换机
        bool remove(const std::string &name)
        {
            // delete from exchange_table where name='name';
            std::stringstream ss;
            ss << "delete from exchange_table where name='" << name << "';";
            return _sql_helper.exec(ss.str(), nullptr, nullptr);
        }

        // 获取所有交换机
        ExchangeMap recovery()
        {
            ExchangeMap result;
            std::string sql = "select name,type,durable,auto_delete,args from exchange_table";
            _sql_helper.exec(sql, selectCallback, &result);
            return result;
        }

    private:
        static int selectCallback(void *arg, int numcol, char **row, char **fields)
        {
            // 将存储数据的容器做类型转换
            ExchangeMap *result = (ExchangeMap *)arg;
            // 构造Exchange所需要的5个参数
            std::string name = row[0];
            ExchangeType type = static_cast<ExchangeType>(std::stoi(row[1]));
            DeliveryMode durable = static_cast<DeliveryMode>(std::stoi(row[2]));
            bool auto_delete = std::stoi(row[3]);
            // std::unordered_map<std::string, std::string> args;
            google::protobuf::Map<std::string, std::string> args;
            // 构造Exchange对象,并交由智能指针管理
            std::shared_ptr<Exchange> exptr = std::make_shared<Exchange>(name, type, durable, auto_delete, args);
            if (numcol == 5 && row[4] != nullptr)
                exptr->setArgs(row[4]);
            // 插入构造好的对象
            result->insert(std::make_pair(name, exptr));
            return 0;
        }

    private:
        SqliteHelper _sql_helper; // 数据库操作句柄
    };

    // 交换机管理类
    class ExchangeManager
    {
    public:
        using ptr = std::shared_ptr<ExchangeManager>;
        // 构造函数
        ExchangeManager(const std::string &dbfile) : _mapper(dbfile)
        {
            _exchanges = _mapper.recovery();
            DBG_LOG("读取交换机数据成功!");
        }

        // 声明交换机
        bool declareExchange(const std::string &name,
                             ExchangeType type,
                             DeliveryMode durable,
                             bool auto_delete,
                             const google::protobuf::Map<std::string, std::string> &args)
        {
            // 申请锁,保证线程安全
            std::unique_lock<std::mutex> lock(_mutex);
            auto it = _exchanges.find(name);
            if (it != _exchanges.end())
            {
                // 如果交换机已经存在就返回
                return true;
            }
            Exchange::ptr exptr = std::make_shared<Exchange>(name, type, durable, auto_delete, args);
            if (durable == DeliveryMode::DURABLE)
            {
                // 持久化标志保存到数据库中
                bool ret = _mapper.insert(exptr);
                if (ret == false)
                    return false;
            }
            _exchanges.insert(std::make_pair(name, exptr));
            return true;
        }

        // 删除交换机
        bool deleteExchange(const std::string &name)
        {
            // 申请锁,保证线程安全
            std::unique_lock<std::mutex> lock(_mutex);
            auto it = _exchanges.find(name);
            if (it == _exchanges.end())
            {
                // 如果交换机不存在就返回
                return true;
            }
            if (it->second->durable == true)
            {
                bool ret = _mapper.remove(name);
                if (ret == false)
                    return false;
            }
            _exchanges.erase(name);
            return true;
        }

        // 获取指定交换机
        Exchange::ptr selectExchange(const std::string& name)
        {
            // 申请锁,保证线程安全
            std::unique_lock<std::mutex> lock(_mutex);
            auto it = _exchanges.find(name);
            if (it == _exchanges.end())
            {
                // 如果交换机不存在就返回
                return Exchange::ptr();
            }
            return it->second;
        }

        // 判断交换机是否存在
        bool exists(const std::string &name)
        {
            // 申请锁,保证线程安全
            std::unique_lock<std::mutex> lock(_mutex);
            auto it = _exchanges.find(name);
            if (it == _exchanges.end())
            {
                return false;
            }
            return true;
        }

        // 获取交换机个数
        size_t size()
        {
            std::unique_lock<std::mutex> lock(_mutex);
            return _exchanges.size();
        }

        // 清理所有交换机数据
        void clear()
        {
            // 申请锁,保证线程安全
            std::unique_lock<std::mutex> lock(_mutex);
            _mapper.removeTable();
            _exchanges.clear();
        }

    private:
        std::mutex _mutex;      // 锁,因为这个类会被多线程访问
        ExchangeMapper _mapper; // 交换机数据持久化管理类
        ExchangeMap _exchanges; // 交换机列表
    };
}

#endif