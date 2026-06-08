#ifndef _M__THREADPOOL__H_
#define _M__THREADPOOL__H_

#include <iostream>
#include <vector>
#include <functional>
#include <memory>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <future>

class Threadpool
{
public:
    using ptr = std::shared_ptr<Threadpool>;
    using Functor = std::function<void(void)>;
    Threadpool(int thr_count = 5) : _stop(false)
    { // 初始化停止标志为假
        for (int i = 0; i < thr_count; i++)
        {
            // 创建线程,线程执行的函数为entry
            _threads.emplace_back(std::thread(&Threadpool::entry, this));
        }
    }
    ~Threadpool()
    {
        stop();
    }

    template <typename F, typename... Args>
    // 传入一个函数,接下来是函数的参数,需要不定参数
    // 函数内部会将传入的参数封装成一个异步任务(packaged_task)
    // 使用lambda生成一个可调用对象(内部执行异步任务),抛入到任务池中,由工作线程取出进行执行
    auto push(F &&func, Args &&...args) -> std::future<decltype(func(args...))>
    {
        // 1.将传入的函数封装成一个packaged_task任务
        using return_type = decltype(func(args...));
        auto func_b = std::bind(std::forward<F>(func), std::forward<Args>(args)...);
        auto task = std::make_shared<std::packaged_task<return_type()>>(func_b);
        std::future<return_type> fu = task->get_future();
        // 2.构造一个lambda匿名函数(捕获任务对象),函数内执行任务对象
        auto lam = [task]()
        {
            (*task)();
        };
        // 3.将构造出来的匿名函数对象抛入到任务池中
        {
            std::unique_lock<std::mutex> lock(_mutex);
            _taskpool.push_back(lam);
        }
        _cv.notify_one();
        return fu;
    }

    void stop()
    {
        _stop = true;
        _cv.notify_all();
        for (std::thread &thr : _threads)
        {
            thr.join();
        }
    }

private:
    // 线程的入口函数,内部不断的从任务池中取出任务进行执行
    void entry()
    {
        while (!_stop)
        {
            std::vector<Functor> tmp_taskpool;
            {
                // 加锁
                std::unique_lock<std::mutex> lock(_mutex);
                // 等待任务池不为空或者_stop为真
                _cv.wait(lock, [this]()
                         { return _stop || !_taskpool.empty(); });
                // 取出任务执行
                tmp_taskpool.swap(_taskpool);
            }
            for (Functor &task : tmp_taskpool)
            {
                task();
            }
        }
    }

private:
    std::atomic<bool> _stop;           // 控制线程启动和停止
    std::vector<Functor> _taskpool;    // 任务队列
    std::vector<std::thread> _threads; // 线程队列
    std::mutex _mutex;                 // 互斥锁
    std::condition_variable _cv;       // 条件变量
};

#endif 