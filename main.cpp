#include <chrono>
#include <condition_variable>
#include <fstream>
#include <iostream>
#include <list>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>


#include "third_party/flat_hash_map.hpp"

typedef ska::flat_hash_map<std::string, size_t> map_type;

const int MAX_WORKERS = 64;
const int LINES_PER_BATCH = 100;

bool inline CheckAndLower(char *c) {
  if ('a' <= *c && *c <= 'z') {
    return true;
  }
  if ('A' <= *c && *c <= 'Z') {
    *c = *c - 'A' + 'a';
    return true;
  }
  return false;
}

void ProcessLine(std::string *line, ska::flat_hash_map<std::string, size_t> *counter) {
  size_t start = 0;
  size_t length = 0;
  for (size_t i = 0; i < line->size(); ++i) {
    if (!CheckAndLower(line->data() + i)) {
      if (length > 0) {
        (*counter)[line->substr(start, length)] += 1;
      }
      start = i + 1;
      length = 0;
    } else {
      ++length;
    }
  }
  if (length > 0) {
    // Assuming insert_or_assign is used with 0 as default value
    (*counter)[line->substr(start, length)] += 1;
  }
}

void ProcessLines(std::list<std::string> &&lines, ska::flat_hash_map<std::string, size_t> *counter) {
  for (auto line: lines) {
    ProcessLine(&line, counter);
  }
}

class ProcessLineThreadPool {
 public:
  ProcessLineThreadPool(size_t num_threads) : counters(num_threads), stop(false) {
    for (size_t i = 0; i < num_threads; ++i) {
      workers.emplace_back(
        [this, i] {
          for (;;) {
            std::list<std::string> task;

            {
              std::unique_lock<std::mutex> lock(this->queue_mutex);
              this->condition.wait(lock,
                                   [this] { return this->stop || !this->tasks.empty(); });
              if (this->stop && this->tasks.empty())
                return;
              task = std::move(this->tasks.front());
              this->tasks.pop();
            }

            ProcessLines(std::move(task), &counters[i]);
          }
        }
      );
    }
  }

  void enqueue(std::list<std::string> &&lines) {
    // TODO: implement queue capacity blocking logic so that thread pool does not
    //       load all the data into ram
    {
      std::unique_lock<std::mutex> lock(queue_mutex);

      // don't allow enqueueing after stopping the pool
      if(stop) {
        throw std::runtime_error("enqueue on stopped ProcessLineThreadPool");
      }

      tasks.emplace(std::move(lines));
    }
    condition.notify_one();
  }

  map_type GatherResults() {
    {
      std::unique_lock<std::mutex> lock(queue_mutex);
      stop = true;
    }
    condition.notify_all();
    for(std::thread &worker: workers) {
      worker.join();
    }
    map_type result;
    for (const auto &c: counters) {
      for (const auto &entry: c) {
        result[entry.first] += entry.second;
      }
    }
    return result;
  }

 private:
  // need to keep track of threads so we can join them
  std::vector<std::thread> workers;
  // the task queue
  std::queue<std::list<std::string>> tasks;

  // synchronization
  std::mutex queue_mutex;
  std::condition_variable condition;
  bool stop;

  // Counters
  std::vector<map_type> counters;
};


int main(int argc, char *argv[]) {
  if (argc < 3 || argc > 4) {
    std::cout << "word-counter INPUT OUTPUT [NUM_WORKERS]" << std::endl;
    std::cout << "Wrong number of arguments, expected 2 or 3" << std::endl;
    return 0;
  }

  auto start = std::chrono::high_resolution_clock::now();
  map_type counter;
  std::ifstream fin(argv[1]);
  std::ofstream fout(argv[2]);

  int num_workers = 1;
  try {
    num_workers = argc == 4 ? std::stoi(argv[3]) : 1;
  } catch (const std::invalid_argument &e) {
    std::cout << "Invalid NUM_WORKERS parameter: " << argv[3] << std::endl;
    return 0;
  }
  if (num_workers < 1 || num_workers > MAX_WORKERS) {
    std::cout << "Invalid number of workers: " << num_workers << std::endl;
    return 0;
  }

  if (num_workers == 1) {
    std::string buffer;
    while (!fin.eof()) {
      std::getline(fin, buffer);
      ProcessLine(&buffer, &counter);
    }
  } else {
    std::list<ska::flat_hash_map<std::string, size_t>> counters(num_workers);
    std::list<std::string> string_batch;
    { // thread pool usage section
      ProcessLineThreadPool thread_pool(num_workers);

      std::string buffer;
      int lines_read = 0;

      while (!fin.eof()) {
        std::getline(fin, buffer);
        string_batch.emplace_back(std::move(buffer));
        if (++lines_read == LINES_PER_BATCH) {
          lines_read = 0;
          counters.emplace_back();
          // TODO: limit the queue size so that the
          thread_pool.enqueue(std::move(string_batch));
        }
      }

      counter = thread_pool.GatherResults();
      ProcessLines(std::move(string_batch), &counter);
    }
  }

  std::vector<std::pair<std::string, size_t>> array;
  array.reserve(counter.size());
  for (auto &iter: counter) {
    array.emplace_back(std::move(iter.first), iter.second);
  }

  std::sort(array.begin(), array.end(),
            [](const std::pair<std::string, size_t> &lhs, const std::pair<std::string, size_t> &rhs) {
              if (lhs.second == rhs.second) return lhs.first < rhs.first;
              return lhs.second > rhs.second;
            });

  for (const auto &entry: array) {
    fout << entry.second << " " << entry.first << std::endl;
  }

  auto duration = std::chrono::high_resolution_clock::now() - start;
  std::cout << std::chrono::duration_cast<std::chrono::milliseconds>(duration).count() * 0.001 << " s." << std::endl;

  return 0;
}
