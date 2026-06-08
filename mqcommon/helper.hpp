#ifndef __M_HELPER_H__
#define __M_HELPER_H__

#include "logger.hpp"
#include <sqlite3.h>
#include <string>
#include <iostream>
#include <vector>
#include <random>
#include <sstream>
#include <iomanip>
#include <atomic>
#include <sys/stat.h>
#include <fstream>
#include <cstring>
#include <cerrno>

namespace mq
{

    class SqliteHelper
    {
    public:
        // 查询语句的回调函数类型,第一个参数为要存储的数据地址,
        // 第二个参数为查询返回多少列
        // 第三个参数是一个字符串数组,保存了查询到的一行的数据
        // 每查询到一行数据就会自动调用一次回调函数
        typedef int (*SqliteCallback)(void *, int, char **, char **);
        SqliteHelper(const std::string &dbfile) : _dbfile(dbfile), _handler(nullptr) {}
        bool open(int safe_leve = SQLITE_OPEN_FULLMUTEX)
        {
            // int sqlite3_open_v2(const char* filename,sqlite3** ppDb,int flags,const char* zVfs);
            int ret = sqlite3_open_v2(_dbfile.c_str(), &_handler, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | safe_leve, nullptr);
            if (ret != SQLITE_OK)
            {
                ERR_LOG("创建或打开数据库失败:%s", sqlite3_errmsg(_handler));
                return false;
            }
            return true;
        }

        bool exec(const std::string &sql, SqliteCallback cb, void *arg)
        {
            // int sqlite3_exec(sqlite3*,char* sql,int(*callback)void*,int,char**,char**),void* arg,char** err);
            int ret = sqlite3_exec(_handler, sql.c_str(), cb, arg, nullptr);
            if (ret != SQLITE_OK)
            {
                ERR_LOG("%s\n 执行语句失败:%s", sql.c_str(), sqlite3_errmsg(_handler));
                return false;
            }
            return true;
        }

        void close()
        {
            // int sqlite3_close_v2(sqlite3*);
            if (_handler)
                sqlite3_close_v2(_handler);
        }

    private:
        std::string _dbfile;
        sqlite3 *_handler;
    };

    class SplitHelper
    {
    public:
        static size_t split(const std::string &str, const std::string &sep, std::vector<std::string> &result)
        {
            if (sep == "")
                return 0;
            size_t pos, idx = 0;
            while (idx < str.size())
            {
                pos = str.find(sep, idx);
                if (pos == std::string::npos)
                {
                    std::string tmp = str.substr(idx);
                    result.push_back(tmp);
                    return result.size();
                }
                else if (idx == pos)
                {
                    idx += sep.size();
                    continue;
                }
                else
                {
                    std::string tmp = str.substr(idx, pos - idx);
                    idx = pos + sep.size();
                    result.push_back(tmp);
                }
            }
            return result.size();
        }
    };

    class UUIDHelper
    {
    public:
        static std::string uuid()
        {
            std::random_device rd;
            std::mt19937_64 generator(rd());
            std::uniform_int_distribution<int> distribution(0, 255);
            std::stringstream ss;
            for (int i = 0; i < 8; i++)
            {
                ss << std::setw(2) << std::setfill('0') << std::hex << distribution(generator);
                if (i == 3 || i == 5 || i == 7)
                    ss << "-";
            }

            static std::atomic<size_t> seq(1);
            size_t num = seq.fetch_add(1);
            for (int i = 7; i >= 0; i--)
            {
                ss << std::setw(2) << std::setfill('0') << std::hex << ((num >> (i * 4)) & 0xff);
                if (i == 6)
                    ss << '-';
            }

            return ss.str();
        }
    };

    class FileHelper
    {
    public:
        FileHelper(const std::string &filename) : _filename(filename) {}
        bool exists()
        {
            struct stat st;
            return (stat(_filename.c_str(), &st) == 0); // stat函数成功返回0,不成功会返回1
        }

        size_t size()
        {
            struct stat st;
            int ret = stat(_filename.c_str(), &st);
            if (ret < 0)
            {
                return 0;
            }
            return st.st_size;
        }

        bool read(char *body, size_t offset, size_t len)
        {
            // 打开文件
            std::ifstream ifs(_filename.c_str(), std::ios::binary | std::ios::in);
            if (ifs.is_open() == false)
            {
                ERR_LOG("%s 文件打开失败", _filename.c_str());
            }
            // 跳到指定位置
            ifs.seekg(offset, std::ios::beg);
            // 读取文件内容
            ifs.read(body, len);
            if (ifs.good() == false)
            {
                ERR_LOG("%s 文件读取失败", _filename.c_str());
                ifs.close();
                return false;
            }
            // 关闭文件
            ifs.close();
            return true;
        }

        bool read(std::string &body)
        {
            // 获取文件大小,根据文件大小调整body的空间
            size_t fsize = this->size();
            body.resize(fsize);
            return read(&body[0], 0, fsize);
        }

        bool write(const char *body, size_t offset, size_t len)
        {
            // 打开文件
            std::fstream fs(_filename, std::ios::binary | std::ios::in | std::ios::out);
            if (fs.is_open() == false)
            {
                ERR_LOG("%s 打开文件失败", _filename.c_str());
            }
            // 跳到指定位置
            fs.seekg(offset, std::ios::beg);
            // 写入文件
            fs.write(body, len);
            if (fs.good() == false)
            {
                ERR_LOG("%s 文件写入失败");
                fs.close();
                return false;
            }
            // 关闭文件
            fs.close();
            return true;
        }

        bool write(const std::string &body)
        {
            return write(body.c_str(), 0, body.size());
        }

        bool rename(const std::string &name)
        {
            return (::rename(_filename.c_str(), name.c_str()) == 0); // 返回0成功,返回非零出错
        }

        static std::string parentDirectory(const std::string &filename)
        {
            //  aaa/bbb/ccc
            size_t pos = filename.find_last_of("/");
            if (pos == std::string::npos)
            {
                return "./";
            }
            return filename.substr(0, pos);
        }

        static bool createFile(const std::string &filename)
        {
            // 打开一个文件,不存在则创建,存在则直接返回
            if (FileHelper(filename).exists())
                return true;
            std::ofstream ofs(filename, std::ios::binary | std::ios::out);
            if (ofs.is_open() == false)
            {
                ERR_LOG("创建%s文件失败", filename.c_str());
                return false;
            }
            return true;
        }

        static bool removeFile(const std::string &filename)
        {
            return (::remove(filename.c_str()) == 0);
        }

        static bool createDirectory(const std::string &path)
        {
            if (FileHelper(path).exists())
                return true;
            //  aaa/bbb/ccc
            size_t idx = 0;
            while (idx < path.size())
            {
                size_t pos = path.find('/', idx);
                if (pos == std::string::npos) // 最后一次,没有找到'/'的情况
                {
                    std::string subpath = path.substr(0);
                    return (mkdir(subpath.c_str(), 0775) == 0);
                }
                std::string subpath = path.substr(0, pos);
                int ret = mkdir(subpath.c_str(), 0775);
                if (ret == 0 && errno != EEXIST)
                {
                    ERR_LOG("创建目录%s失败: %s", subpath.c_str(), strerror(errno));
                    return false;
                }
                idx = pos + 1;
            }
            return true;
        }

        static bool removeDirectory(const std::string &path)
        {
            std::string cmd = "rm -rf " + path;
            return (system(cmd.c_str()) != -1);
        }

    private:
        std::string _filename;
    };
}

#endif