#ifndef _M__ROUTE_H__
#define _M__ROUTE_H__

#include "../mqcommon/helper.hpp"
#include "../mqcommon/msg.pb.h"

namespace mq
{
    class router
    {
    public:
        static bool isLegalRoutingKey(const std::string &routing_key)
        {
            // 只需判断是否包含非法字符,合法字符(a~z,A~Z,0~9,.,_)
            for (const char &ch : routing_key)
            {
                if ((ch >= 'a' && ch <= 'z') ||
                    (ch >= 'A' && ch <= 'Z') ||
                    (ch >= '0' && ch <= '9') ||
                    (ch == '.') ||
                    (ch == '_'))
                {
                    continue;
                }
                return false;
            }
            return true;
        }
        static bool isLegalBindingKey(const std::string &binding_key)
        {
            // 1.判断是否包含非法字符,合法字符(a~z,A~Z,0~9,.,_,*,#)
            for (const char &ch : binding_key)
            {
                if ((ch >= 'a' && ch <= 'z') ||
                    (ch >= 'A' && ch <= 'Z') ||
                    (ch >= '0' && ch <= '9') ||
                    (ch == '.') ||
                    (ch == '_') ||
                    (ch == '*') ||
                    (ch == '#'))
                {
                    continue;
                }
                return false;
            }
            // 2.#和*必须单独出现
            std::vector<std::string> sub_words;
            SplitHelper::split(binding_key, ".", sub_words);
            for (const std::string &str : sub_words)
            {
                if (str.size() > 1 && (str.find("*") != std::string::npos || str.find("#") != std::string::npos))
                {
                    return false;
                }
            }
            // 3.#和*不能连续出现
            for (int i = 1; i < sub_words.size(); ++i)
            {
                if (sub_words[i] == "*" && sub_words[i - 1] == "#")
                {
                    return false;
                }
                if (sub_words[i] == "#" && sub_words[i - 1] == "#")
                {
                    return false;
                }
                if (sub_words[i] == "#" && sub_words[i - 1] == "*")
                {
                    return false;
                }
            }
            return true;
        }
        static bool route(ExchangeType type, const std::string &binding_key, const std::string &routing_key)
        {
            if(isLegalRoutingKey(routing_key)==false){
                DBG_LOG("routing_key不合法!%s",routing_key.c_str());
                return false;
            }
            if(isLegalBindingKey(binding_key)==false){
                DBG_LOG("binding_key不合法!%s",binding_key.c_str());
                return false;
            }
            if (type == ExchangeType::DIRECT)
            {
                return (routing_key == binding_key);
            }
            else if (type == ExchangeType::FANOUT)
            {
                return true;
            }
            else if (type == ExchangeType::TOPIC)
            {
                // 主题交换:要进行模式匹配 news.# news.music.pop
                std::vector<std::string> sub_binding_key;
                SplitHelper::split(binding_key, ".", sub_binding_key);
                std::vector<std::string> sub_routing_key;
                SplitHelper::split(routing_key, ".", sub_routing_key);
                
                std::vector<std::vector<int>> vv(sub_binding_key.size() + 1, std::vector<int>(sub_routing_key.size() + 1, 0));
                vv[0][0] = 1;
                if(sub_binding_key[0]=="#")
                        vv[1][0]=1;
                for (int i = 1; i < sub_binding_key.size() + 1; ++i)//i=1 j=3
                {
                    for (int j = 1; j < sub_routing_key.size() + 1; ++j)
                    {
                        if(sub_binding_key[i-1]=="#"){
                            vv[i][j] = vv[i-1][j-1]||vv[i-1][j]||vv[i][j-1];
                        }
                        else if(sub_binding_key[i-1]=="*"){
                            vv[i][j] = vv[i-1][j-1];
                        }
                        else{
                            vv[i][j] = (sub_binding_key[i-1]==sub_routing_key[j-1])&&vv[i-1][j-1];
                        }
                    }
                    
                }
                return vv[sub_binding_key.size()][sub_routing_key.size()];
            }
            else{
                DBG_LOG("交换机格式错误!");
                return false;
            }
        }
    };
}

#endif