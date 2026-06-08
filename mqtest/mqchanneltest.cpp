#include "../mqserver/channel.hpp"
//mq::ChannelManager::ptr cmptr;
int main()
{
    mq::ChannelManager::ptr cmptr = std::make_shared<mq::ChannelManager>();
    cmptr->openChannel("1",muduo::net::TcpConnectionPtr(),mq::ProtobufCodecPtr(),mq::ConsumerManager::ptr(),mq::VirtualHost::ptr(),Threadpool::ptr());
    return 0;
}