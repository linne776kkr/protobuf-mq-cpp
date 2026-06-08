#include"../mqserver/connection.hpp"

int main(){
    mq::ConnectionManager::ptr cmp = std::make_shared<mq::ConnectionManager>();
    cmp->newConnection(muduo::net::TcpConnectionPtr(),mq::ProtobufCodecPtr(),mq::ConsumerManager::ptr(),mq::VirtualHost::ptr(),Threadpool::ptr());
    return 0;
}