// #include "../include/muduo/proto/codec.h"
// #include "../include/muduo/proto/dispatcher.h"

// #include "request.pb.h"

// #include "muduo/base/Logging.h"
// #include "muduo/base/Mutex.h"
// #include "muduo/net/EventLoop.h"
// #include "muduo/net/TcpServer.h"


#include "muduo/proto/codec.h"
#include "muduo/proto/dispatcher.h"

#include "request.pb.h"

#include "muduo/base/Logging.h"
#include "muduo/base/Mutex.h"
#include "muduo/net/EventLoop.h"
#include "muduo/net/TcpServer.h"
class server
{
public:
    typedef std::shared_ptr<text::AddRequest> AddRequestPtr;
    typedef std::shared_ptr<text::AddResponse> AddResponsePtr;
    typedef std::shared_ptr<text::TranslateRequest> TranslateRequestPtr;
    typedef std::shared_ptr<text::TranslateResponse> TranslateResponsePtr;

    server(int port) : _server(&_loop, muduo::net::InetAddress("0.0.0.0", port), "server", muduo::net::TcpServer::kNoReusePort),
                       _dispatcher(std::bind(&server::onUnknownMessage, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3)),
                       _codec(std::bind(&ProtobufDispatcher::onProtobufMessage, &_dispatcher,
                                        std::placeholders::_1, std::placeholders::_2, std::placeholders::_3))
    {
        // 注册业务处理函数
        _dispatcher.registerMessageCallback<text::AddRequest>(
            std::bind(&server::onAdd, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
        _dispatcher.registerMessageCallback<text::TranslateRequest>(
            std::bind(&server::onTranslate, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
        // 设置连接回调函数和信息回调函数
        _server.setMessageCallback(
            std::bind(&ProtobufCodec::onMessage, &_codec, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
        _server.setConnectionCallback(
            std::bind(&server::onConnection, this, std::placeholders::_1));
    }

    void start()
    {
        _server.start();
        _loop.loop();
    }

private:
    // 未知消息处理函数
    void onUnknownMessage(const muduo::net::TcpConnectionPtr &conn, const MessagePtr &message, muduo::Timestamp)
    {
        LOG_INFO << "onUnknownMessage: " << message->GetTypeName();
        conn->shutdown();
    }

    // 加法业务处理函数
    void onAdd(const muduo::net::TcpConnectionPtr &conn, const AddRequestPtr &message, muduo::Timestamp)
    {
        // 获取请求信息
        int num1 = message->num1();
        int num2 = message->num2();
        // 构造返回信息
        text::AddResponse resp;
        resp.set_result(num1 + num2);
        // 发送返回信息
        _codec.send(conn, resp);
    }

    // 翻译业务处理函数
    void onTranslate(const muduo::net::TcpConnectionPtr &conn, const TranslateRequestPtr &message, muduo::Timestamp)
    {
        std::string str = message->msg();
        text::TranslateResponse resp;
        if (str == "1")
            resp.set_msg("影色舞");
        else if (str == "2")
            resp.set_msg("迷星叫");
        else if (str == "3")
            resp.set_msg("诗超绊");
        else if (str == "4")
            resp.set_msg("名无声");
        else if (str == "5")
            resp.set_msg("无路矢");
        else
            resp.set_msg("没有了");
        _codec.send(conn, resp);
    }

    // 连接成功或关闭时回调函数
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

private:
    muduo::net::EventLoop _loop;
    muduo::net::TcpServer _server;  // 服务器对象
    ProtobufDispatcher _dispatcher; // 请求分发器对象--要向其中注册请求处理函数
    ProtobufCodec _codec;           // protobuf协议处理器--针对收到的请求进行protobuf协议处理
};

int main()
{
    server yi(8888);
    yi.start();
    return 0;
}