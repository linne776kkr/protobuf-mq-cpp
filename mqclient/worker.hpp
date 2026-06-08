#ifndef __M_WORKER_H__
#define __M_WORKER_H__

#include "muduo/net/EventLoopThread.h"

#include "../mqcommon/threadpool.hpp"
#include "../mqcommon/helper.hpp"

namespace mq
{
    class AsyncWorker
    {
    public:
        using ptr = std::shared_ptr<AsyncWorker>;
        muduo::net::EventLoopThread loopthread;
        Threadpool pool;
    };
}

#endif