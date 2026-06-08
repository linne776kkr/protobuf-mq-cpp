#include "../include/muduo/net/TcpClient.h"
#include "../include/muduo/net/EventLoopThread.h"
#include "../include/muduo/net/TcpConnection.h"
#include "../include/muduo/base/CountDownLatch.h"

#include <iostream>
#include <functional>

class TranaslateClint
{
public:
    TranaslateClint(const std::string &ip, int port) // 构造函数
        : _latch(1),
          _client(_loopthread.startLoop(),
                  muduo::net::InetAddress(ip, port),
                  "TranslateClient")
    {
        _client.setConnectionCallback(std::bind(&TranaslateClint::onConnection, this, std::placeholders::_1));
        _client.setMessageCallback(std::bind(&TranaslateClint::onMessage, this,
                                             std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
    }
    // 连接服务器,需阻塞等待唤醒
    void connect()
    {
        _client.connect();
        _latch.wait();
    }
    
    void send(const std::string &msg)
    {
        if (_conn->connected() == false)
        {
            std::cout << "发送失败" << std::endl;
        }
        else
        {
            _conn->send(msg);
        }
    }

private:
    // 连接成功时的回调函数,连接成功后,唤醒上方阻塞
    void onConnection(const muduo::net::TcpConnectionPtr &conn)
    {
        if (conn->connected() == true)
        {
            _conn = conn;
            _latch.countDown();
            std::cout << "连接成功" << std::endl;
        }
        else
        {
            std::cout << "连接已断开" << std::endl;
        }
    }
    // 收到消息时的回调函数
    void onMessage(const muduo::net::TcpConnectionPtr &conn, muduo::net::Buffer *buf, muduo::Timestamp)
    {
        std::string str;
        str = buf->retrieveAllAsString();
        std::cout << str << std::endl;
    }

private:
    muduo::CountDownLatch _latch;            // 用于程序等待
    muduo::net::EventLoopThread _loopthread; // 线程阻塞等待信息
    muduo::net::TcpClient _client;           // 客户端对象
    muduo::net::TcpConnectionPtr _conn;      // 客户端的信息
};

int main()
{
    TranaslateClint clint("127.0.0.1", 8888);
    clint.connect();
    while (1)
    {
        std::string str;
        std::cin >> str;
        clint.send(str);
    }
    return 0;
}