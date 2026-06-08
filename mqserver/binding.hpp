#ifndef __M_BINDING_H__
#define __M_BINDING_H_

#include "../mqcommon/helper.hpp"
#include "../mqcommon/msg.pb.h"
#include <unordered_map>
#include <mutex>
#include <memory>
#include <assert.h>

namespace mq
{
    struct Binding
    {
        using ptr = std::shared_ptr<Binding>;
        std::string exchange_name; // 交换机的名字
        std::string msgqueue_name; // 队列的名字
        std::string binding_key;   // 绑定信息中的关键字

        Binding() {};
        Binding(const std::string &ename,
                const std::string &qname,
                const std::string &key) : exchange_name(ename), msgqueue_name(qname), binding_key(key) {}
    };

    // 队列名称-绑定信息,通过队列的名称能够找到唯一对应的绑定信息
    using MsgQueueBindingMap = std::unordered_map<std::string, Binding::ptr>;
    // 交换机名称-队列名称和绑定信息的哈希表,通过交换机名称能够找到所有绑定在该交换机的绑定信息
    using BindingMap = std::unordered_map<std::string, MsgQueueBindingMap>;

    class BindingMapper
    {
    public:
        // 构造函数
        BindingMapper(const std::string &dbfile) : _sql_helper(dbfile)
        {
            std::string path = FileHelper::parentDirectory(dbfile);
            FileHelper::createDirectory(path);
            assert(_sql_helper.open());
            createTable();
        }
        // 创建表
        void createTable()
        {
            // create table if not exists binding_table(exchange_name varchar(32),msgqueue_name varchar(32),binding_key varchar(128));
            std::stringstream sql;
            sql << "create table if not exists binding_table(";
            sql << "exchange_name varchar(32),";
            sql << "msgqueue_name varchar(32),";
            sql << "binding_key varchar(128));";
            assert(_sql_helper.exec(sql.str(), nullptr, nullptr));
        }
        // 删除表
        void removeTable()
        {
            // drop table if exists binding_table;
            std::string sql = "drop table if exists binding_table;";
            assert(_sql_helper.exec(sql, nullptr, nullptr));
        }
        // 插入绑定信息
        bool insert(Binding::ptr &binding)
        {
            // insert into binding_table values('exchangename','msgqueuename','binding_key');
            std::stringstream sql;
            sql << "insert into binding_table values(";
            sql << "'" << binding->exchange_name << "',";
            sql << "'" << binding->msgqueue_name << "',";
            sql << "'" << binding->binding_key << "');";
            return _sql_helper.exec(sql.str(), nullptr, nullptr);
        }
        // 删除指定绑定信息
        bool remove(const std::string &ename, const std::string &qname)
        {
            // delete from binding_table where exchange_name='' and msgqueue_name='';
            std::stringstream sql;
            sql << "delete from binding_table where ";
            sql << "exchange_name='" << ename << "' and ";
            sql << "msgqueue_name='" << qname << "';";
            return _sql_helper.exec(sql.str(), nullptr, nullptr);
        }
        // 删除指定交换机的绑定信息,删除交换机时调用
        bool removeExchangeBinding(const std::string &ename)
        {
            // delete from binding_table where exchange_name='';
            std::stringstream sql;
            sql << "delete from binding_table where ";
            sql << "exchange_name='" << ename << "';";
            return _sql_helper.exec(sql.str(), nullptr, nullptr);
        }
        // 删除指定队列的所有绑定信息,删除队列时调用
        bool removeMsgQueueBinding(const std::string &qname)
        {
            // delete from binding_table where msgqueue_name='';
            std::stringstream sql;
            sql << "delete from binding_table where ";
            sql << "msgqueue_name='" << qname << "';";
            return _sql_helper.exec(sql.str(), nullptr, nullptr);
        }
        // 获取所有绑定信息,用于重启服务器时恢复数据
        BindingMap recovery()
        {
            BindingMap result;
            // select exchange_name,msgqueue_name,binding_key from binding_table;
            std::string sql = "select exchange_name,msgqueue_name,binding_key from binding_table;";
            _sql_helper.exec(sql, selectCallback, &result);
            return result;
        }

    private:
        static int selectCallback(void *arg, int numcol, char **row, char **fields)
        {
            BindingMap *result = (BindingMap *)arg;
            Binding::ptr bptr = std::make_shared<Binding>(row[0], row[1], row[2]);
            MsgQueueBindingMap &qmap = (*result)[bptr->exchange_name];
            qmap.insert(std::make_pair(bptr->msgqueue_name, bptr));
            return 0;
        }

    private:
        SqliteHelper _sql_helper; // 数据库操作句柄
    };

    class BindingManager
    {
    public:
        using ptr = std::shared_ptr<BindingManager>;
        BindingManager(const std::string &dbfile) : _mapper(dbfile)
        {
            _bindings = _mapper.recovery();
            DBG_LOG("读取绑定信息成功!");
        }
        // 新增绑定
        bool bind(const std::string &ename, const std::string &qname, const std::string &key, bool durable)
        {
            std::unique_lock<std::mutex> lock(_mutex);
            BindingMap::iterator eit = _bindings.find(ename);
            if (eit != _bindings.end())
            {
                MsgQueueBindingMap::iterator qit = eit->second.find(qname);
                if (qit != eit->second.end())
                {
                    return true;
                }
            }
            Binding::ptr bptr = std::make_shared<Binding>(ename, qname, key);
            if (durable)
            {
                bool ret = _mapper.insert(bptr);
                if (ret == false)
                    return false;
            }
            _bindings[ename][qname] = bptr;
            return true;
        }
        // 解除绑定
        void unBind(const std::string &ename, const std::string &qname)
        {
            std::unique_lock<std::mutex> lock(_mutex);
            BindingMap::iterator eit = _bindings.find(ename);
            if (eit == _bindings.end())
            {
                return;
            }
            MsgQueueBindingMap::iterator qit = eit->second.find(qname);
            if (qit == eit->second.end())
            {
                return;
            }
            _mapper.remove(ename, qname);
            _bindings[ename].erase(qname);
        }
        // 解除交换机所有绑定
        void removeExchangeBindings(const std::string &ename)
        {
            std::unique_lock<std::mutex> lock(_mutex);
            BindingMap::iterator eit = _bindings.find(ename);
            if (eit == _bindings.end())
            {
                return;
            }
            _mapper.removeExchangeBinding(ename);
            _bindings.erase(ename);
        }
        // 解除队列所有绑定
        void removeMsgQueueBindings(const std::string &qname)
        {
            std::unique_lock<std::mutex> lock(_mutex);
            for (BindingMap::iterator start = _bindings.begin(); start != _bindings.end(); ++start)
            {
                start->second.erase(qname);
            }
            _mapper.removeMsgQueueBinding(qname);
        }
        // 获取交换机的所有绑定信息
        MsgQueueBindingMap getExchangeBindings(const std::string &ename)
        {
            std::unique_lock<std::mutex> lock(_mutex);
            BindingMap::iterator eit = _bindings.find(ename);
            if (eit == _bindings.end())
            {
                return MsgQueueBindingMap();
            }
            return eit->second;
        }
        //获取指定绑定信息
        Binding::ptr getBinding(const std::string &ename, const std::string &qname)
        {
            std::unique_lock<std::mutex> lock(_mutex);
            BindingMap::iterator eit = _bindings.find(ename);
            if (eit == _bindings.end())
            {
                return nullptr;
            }
            MsgQueueBindingMap::iterator qit = eit->second.find(qname);
            if (qit == eit->second.end())
            {
                return nullptr;
            }
            return qit->second;
        }
        //判断指定绑定信息是否存在
        bool exists(const std::string &ename, const std::string &qname){
            std::unique_lock<std::mutex> lock(_mutex);
            BindingMap::iterator eit = _bindings.find(ename);
            if (eit == _bindings.end())
            {
                return false;
            }
            MsgQueueBindingMap::iterator qit = eit->second.find(qname);
            if (qit == eit->second.end())
            {
                return false;
            }
            return true;
        }
        //获取绑定信息的数量
        size_t size(){
            std::unique_lock<std::mutex> lock(_mutex);
            size_t total_size = 0;
            for (BindingMap::iterator start = _bindings.begin(); start != _bindings.end(); ++start)
            {
                total_size+=start->second.size();
            }
            return total_size;
        }
        //清理所有绑定信息
        void clear(){
            std::unique_lock<std::mutex> lock(_mutex);
            _mapper.removeTable();
            _bindings.clear();
        }

    private:
        std::mutex _mutex;
        BindingMapper _mapper; // 绑定信息持久化管理类
        BindingMap _bindings;  // 绑定信息管理
    };
}

#endif