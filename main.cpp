
struct task_t
{
  void operator()();
};

struct th_pool_t
{
  using f_t = std::function< void(void) >;
  explicit th_pool_t(unsigned int k);
  ~th_pool_t();
  void submit(f_t f);

  void pause();
  void resume();
private:
  void execute();
  std::queue< f_t > tasks_;
  std::mutex mx_;
  std::condition_variable cd_;
  bool stop_{false};
  bool paused_{false};
  std::vector< std::thread > ths_;
};

void th_pool_t::submit(f_t f)
{
  mx_.lock();
  tasks_.push(std::move(f));
  mx_.unlock();
  cd_.notify_one();
}

th_pool_t::th_pool_t(unsigned int k) 
{
  auto thf = [this]() { execute(); };
  for (; k; --k)
  {
    ths_.emplace_back(thf);
  }
}

th_pool_t::~th_pool_t()
{
  mx_.lock();
  stop_ = true;
  mx_.unlock();
  cd_.notify_all();
  for (auto && th: ths_)
  {
    th.join();
  }
}

void th_pool_t::execute()
{
  std::unique_lock lck(mx_);
  do {
    auto proceed = [&]() {
      return (!tasks_.empty() && !paused_) || stop_;
    };
    cd_.wait(lck, proceed);

    if (stop_ || paused_) {
      continue;
    }

    if (tasks_.empty()) continue;

    auto task = std::move(tasks_.front());
    tasks_.pop();
    lck.unlock();
    try {
      task();
    }
    catch (...) {}
    lck.lock();
  } while (!stop_ || !tasks_.empty());
}

void th_pool_t::pause() {
  std::unique_lock<std::mutex> lock(mx_);
  paused_ = true;
}

void th_pool_t::resume() {
  {
    std::unique_lock<std::mutex> lock(mx_);
    paused_ = false;
  }
  cd_.notify_all();
}

int main() {}
