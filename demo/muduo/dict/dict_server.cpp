#include "../include/muduo/net/TcpServer.h"
#include "../include/muduo/net/EventLoop.h"
#include "../include/muduo/net/TcpConnection.h"

#include <iostream>
#include <functional>

class TranslateServer
{
public:
    TranslateServer(int port) 
    : _server(&_loop,
    muduo::net::InetAddress("0.0.0.0", port),
    "TranslateServer",
    muduo::net::TcpServer::kReusePort)
    {
        // 通过函数适配器绑定回调函数
        _server.setConnectionCallback(std::bind(&TranslateServer::onConnection, this, std::placeholders::_1));
        _server.setMessageCallback(std::bind(&TranslateServer::onMessage, this,
         std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
    }
    void star()
    {
        // 启动服务器
        _server.start(); // 开始事件监听
        _loop.loop();    // 开始事件监控
    };

private:
    void onConnection(const muduo::net::TcpConnectionPtr &conn)
    {
        // 新连接建立时调用
        if (conn->connected() == true)
        {
            std::cout << "新连接建立成功" << std::endl;
        }
        else
        {
            std::cout << "连接已关闭" << std::endl;
        }
    }
    void onMessage(const muduo::net::TcpConnectionPtr &conn, muduo::net::Buffer *buf, muduo::Timestamp)
    {
        // 通信连接收到请求时的回调函数
        // 1.从buf中吧请求的数据读取出来
        std::string str = buf->retrieveAllAsString();
        // 2.构建返回信息
        std::cout<<"获取到新请求"<<str<<std::endl;
        std::string ret;
        if (str == "1")
            ret = "影色舞";
        else if (str == "2")
            ret = "迷星叫";
        else if (str == "3")
            ret = "诗超绊";
        else if (str == "4")
            ret = "名无声";
        else if (str == "5")
            ret = "无路矢";
        else
            ret = "没有了";
        // 3.对客户端进行结果响应
        conn->send(ret);
    }

private:
    muduo::net::EventLoop _loop;
    muduo::net::TcpServer _server;
};

int main()
{
    TranslateServer sever(8888);
    sever.star();
    return 0;
}