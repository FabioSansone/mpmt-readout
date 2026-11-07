// C++ implementation of thread-safe queue

#include <condition_variable>
#include <iostream>
#include <mutex>
#include <queue>
#include <cstddef>

// Thread-safe queue
template <typename T>
class TSQueue {
private:
   // Underlying queue
   std::queue<T> m_queue;

   // mutex for thread synchronization
   std::mutex m_mutex;

   // Condition variable for signaling
   std::condition_variable m_cond;

   bool keep_running = true;

public:
   // Pushes an element to the queue
   bool push(T item)
   {

      // Acquire lock
      std::lock_guard<std::mutex> lock(m_mutex);

      if (!keep_running)
         return false;

      // Add item
      m_queue.push(item);

      // Notify one thread that
      // is waiting
      m_cond.notify_one();

      return true;
   }

   // Pops an element off the queue
   bool pop(T& item)
   {

      // acquire lock
      std::unique_lock<std::mutex> lock(m_mutex);

      // wait until queue is not empty
      m_cond.wait(lock,
              [this]() { return (!m_queue.empty() || !keep_running); });

      if (!keep_running)
         return false;

      // retrieve item
      item = m_queue.front();
      m_queue.pop();

      // return item
      return true;
   }

   std::size_t size() { 
      std::lock_guard<std::mutex> lock(m_mutex);
      return m_queue.size(); 
   }

   void stop() { 
      {
         std::lock_guard<std::mutex> lock(m_mutex);
         keep_running = false; 
         
      }  

      m_cond.notify_all(); 
   }


   void reset() {
      std::lock_guard<std::mutex> lock(m_mutex);
      std::queue<T> empty;
      m_queue.swap(empty);

      keep_running = true;
   }
};

#if 0

// Driver code
int main()
{
    TSQueue<int> q;

    q.push(1);
    q.push(2);
    q.push(3);

    int valore = 0;

    for (int i = 0; i < 3; ++i) {
        if (q.pop(valore))
            std::cout << valore << '\n';
    }

    q.stop();

    if (!q.pop(valore))
        std::cout << "Coda fermata\n";

    return 0;
}

#endif
