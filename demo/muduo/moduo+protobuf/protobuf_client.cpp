// #include "../include/muduo/proto/codec.h"
// #include "../include/muduo/proto/dispatcher.h"

// #include "request.pb.h"

// #include "muduo/base/Logging.h"
// #include "muduo/base/Mutex.h"
// #include "muduo/net/EventLoop.h"
// #include "muduo/net/TcpClient.h"

// #include "../include/muduo/base/CountDownLatch.h"
// #include "../include/muduo/net/EventLoopThread.h"

#include "muduo/proto/codec.h"
#include "muduo/proto/dispatcher.h"

#include "request.pb.h"

#include "muduo/base/Logging.h"
#include "muduo/base/Mutex.h"
#include "muduo/net/EventLoop.h"
#include "muduo/net/TcpClient.h"

#include "muduo/base/CountDownLatch.h"
#include "muduo/net/EventLoopThread.h"

class client
{
public:
    typedef std::shared_ptr<text::AddResponse> AddResponsePtr;
    typedef std::shared_ptr<text::TranslateResponse> TranslateResponsePtr;

    client(std::string ip, int port)
        : _latch(1),
          _client(_loopthread.startLoop(), muduo::net::InetAddress(ip, port), "client"),
          _dispatcher(std::bind(&client::onUnknownMessage, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3)),
          _codec(std::bind(&ProtobufDispatcher::onProtobufMessage, &_dispatcher, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3))
    {
        // 注册消息分发器
        _dispatcher.registerMessageCallback<text::AddResponse>(
            std::bind(&client::onAddRespone, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
        _dispatcher.registerMessageCallback<text::TranslateResponse>(
            std::bind(&client::onTranslateRespone, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
        // 绑定连接回调函数和消息回调函数
        _client.setConnectionCallback(
            std::bind(&client::onConnection, this, std::placeholders::_1));
        _client.setMessageCallback(
            std::bind(&ProtobufCodec::onMessage, &_codec, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
    }

    // 连接服务器,在成功连接前需要等待
    void connect()
    {
        _client.connect();
        _latch.wait();
    }

    // 发送消息到服务器
    void Add(int num1, int num2)
    {
        // 构造消息格式
        text::AddRequest req;
        req.set_num1(num1);
        req.set_num2(num2);
        // 发送消息
        _codec.send(_conn, req);
    }
    void Translate(std::string str)
    {
        // 构造消息格式
        text::TranslateRequest req;
        req.set_msg(str);
        // 发送消息
        _codec.send(_conn, req);
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
    // 收到消息后调用的函数
    void onUnknownMessage(const muduo::net::TcpConnectionPtr &conn, const MessagePtr &message, muduo::Timestamp)
    {
        LOG_INFO << "onUnknownMessage: " << message->GetTypeName();
    }
    void onAddRespone(const muduo::net::TcpConnectionPtr &conn, const AddResponsePtr &message, muduo::Timestamp)
    {
        std::cout << "收到加法消息:" << message->result() << std::endl;
    }
    void onTranslateRespone(const muduo::net::TcpConnectionPtr &conn, const TranslateResponsePtr &message, muduo::Timestamp)
    {
        std::cout << "收到翻译消息:" << message->msg() << std::endl;
    }

private:
    muduo::CountDownLatch _latch;            // 实现同步的
    muduo::net::EventLoopThread _loopthread; // 异步循环处理线程
    muduo::net::TcpConnectionPtr _conn;      // 客户端对应的连接
    muduo::net::TcpClient _client;           // 客户端对象
    ProtobufDispatcher _dispatcher;          // 请求分发器
    ProtobufCodec _codec;                    // 协议处理器
};

int main()
{
    client fufu("127.0.0.1", 8888);
    fufu.connect();
    while (1)
    {
        std::cout << "选择功能,1为加法,2为翻译,0为退出" << std::endl;
        int choose;
        std::cin >> choose;
        switch (choose)
        {
        case 1:
        {
            std::cout << "输入两个数字" << std::endl;
            int num1, num2;
            std::cin >> num1 >> num2;
            fufu.Add(num1, num2);
            break;
        }
        case 2:
        {
            std::cout << "输入要翻译的字符串" << std::endl;
            std::string str;
            std::cin >> str;
            fufu.Translate(str);
            break;
        }
        case 0:
        {
            exit(0);
        }
        default:
            std::cout << "不对" << std::endl;
        }
    }
    return 0;
}