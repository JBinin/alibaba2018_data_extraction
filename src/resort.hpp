#include <algorithm>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <string>
#include <thread>
#include <vector>
#include <mutex>
class LoserTree {
public:
  int n;
  std::string min_k = "0";
  std::string max_k = "2147483647";
  // node 0 to save winner
  std::vector<int> loser_tree;
  // node 0 to save min value
  std::vector<std::string> leaves;

  std::vector<std::vector<std::string>> &time_stamps;
  std::vector<std::vector<std::string>> &cpu_util_percents;
  std::vector<int> index;

  LoserTree(std::vector<std::vector<std::string>> &time_stamps,
            std::vector<std::vector<std::string>> &cpu_util_percents)
      : time_stamps(time_stamps), cpu_util_percents(cpu_util_percents) {
    n = time_stamps.size();
    for (int i = 0; i < n; ++i)
      time_stamps[i].push_back(max_k);

    index = std::vector<int>(n, 0);
    loser_tree = std::vector<int>(n, 0);
    leaves = std::vector<std::string>(n + 1, min_k);

    for (int i = 0; i < n; ++i)
      loser_tree[i] = n;
    for (int i = 0; i < n; ++i)
      intput(i);
    for (int i = n - 1; i >= 0; --i)
      adjust(i);
  }

  void merge_sort(std::vector<std::string> &result) {
    result.clear();
    std::string time_stamp, cpu_util_percent;
    int i = loser_tree[0];
    while (leaves[i] != max_k) {
      time_stamp = time_stamps[i][index[i] - 1];
      cpu_util_percent = cpu_util_percents[i][index[i] - 1];
      intput(i);
      adjust(i);
      i = loser_tree[0];
      result.push_back(time_stamp + "," + cpu_util_percent);
    }

    for (int i = 0; i < time_stamps.size(); ++i)
      time_stamps[i].pop_back();
  }

  void adjust(int i) {
    int parent = (i + n) / 2;
    while (parent > 0) {
      if (std::stoi(leaves[i]) > std::stoi(leaves[loser_tree[parent]])) {
        std::swap(i, loser_tree[parent]);
      }
      parent /= 2;
    }
    loser_tree[0] = i;
  }

  void intput(int i) {
    leaves[i] = time_stamps[i][index[i]];
    index[i]++;
  }
};

class Resort {
public:
  std::string root_path;
  int thread_count;
  std::mutex m_lock;
  int container_id_l;
  int container_id_h;

  std::vector<int> container_not_exist;
  std::map<int, double> container_part_data_missing;
  std::mutex m_not_exit;
  std::mutex m_data_missing;

  Resort(std::string root_path, int container_id_l = 0,
         int container_id_h = 2147483647, int thread_count = 24)
      : root_path(root_path), container_id_l(container_id_l),
        container_id_h(container_id_h), thread_count(thread_count) {
    if (this->root_path.back() == '/') {
      this->root_path = this->root_path.substr(0, this->root_path.size() - 1);
    }
  }

  // sort range [id_from, id_to)
  // implement in multi thread
  void sort(int size = 10, int id_from = 0, int id_to = 0) {
    if (id_from != 0 or id_to != 0) {
      container_id_l = id_from;
      container_id_h = id_to;
    }
    std::vector<std::thread> threads;
    for (int i = 0; i < thread_count; ++i) {
      threads.push_back(std::thread(&Resort::sort_files, this, size));
    }

    for (int i = 0; i < threads.size(); ++i) {
      threads[i].join();
    }
  }

  // sort files
  // size: file counts for each invocation
  void sort_files(int size) {
    while (true) {
      m_lock.lock();
      if (container_id_l >= container_id_h) {
        m_lock.unlock();
        return;
      }
      int l = container_id_l;
      int h = std::min(container_id_l + size, container_id_h);
      container_id_l = h;

      m_lock.unlock();

      for (int i = l; i < h; ++i) {
        sort_one_file(i);
      }
    }
  }

  void sort_one_file(int container_id) {
    std::string file_path = get_container_path(container_id);

    std::ifstream infile(file_path);
    if (!infile.is_open()) {
      // conainer not exist
      m_not_exit.lock();
      container_not_exist.push_back(container_id);
      m_not_exit.unlock();
      return;
    }
    std::vector<std::vector<std::string>> time_stamps;
    std::vector<std::vector<std::string>> cpu_util_percents;
    std::string line;
    while (infile) {
      std::getline(infile, line);
      time_stamps.push_back(split_str(line));
      std::getline(infile, line);
      cpu_util_percents.push_back(split_str(line));
    }

    // sort
    std::vector<std::string> result;
    LoserTree loset(time_stamps, cpu_util_percents);
    loset.merge_sort(result);
    // save sorted file
    std::string save_file = get_container_save_path(container_id);
    // create directory if not exist
    std::filesystem::path p(save_file);
    std::filesystem::create_directory(p.parent_path());

    std::ofstream outfile(save_file);
    for (int i = 0; i < result.size(); ++i)
      outfile << result[i] << std::endl;
    outfile.close();

    // if data is not complete
    // 8 days [86400, 777600)
    // (777600 - 86400) / 10 = 69120
    if (result.size() != 69120) {
      m_data_missing.lock();
      container_part_data_missing[container_id] = result.size() / 69120.0;
      m_data_missing.unlock();
    }
  }

  void output(int total) {
    std::sort(container_not_exist.begin(), container_not_exist.end());
    std::string outfile = root_path + "/container_not_exist.csv";
    std::ofstream out1(outfile);
    for (auto it = container_not_exist.begin(); it != container_not_exist.end();
         it++) {
      out1 << *it << std::endl;
    }
    out1.close();

    std::cout << "Actually get " << total - container_not_exist.size() << " / "
              << total << " container data." << std::endl;
    std::cout << "See detail in :" << outfile << std::endl;

    outfile = root_path + "/container_part_data_missing.csv";
    std::ofstream out2(outfile);
    for (auto it = container_part_data_missing.begin();
         it != container_part_data_missing.end(); ++it) {
      out2 << it->first << "," << int(it->second * 100) << std::endl;
    }
    out2.close();

    std::cout << "Actually get "
              << total - container_part_data_missing.size() -
                     container_not_exist.size()
              << " / " << total << " complete container data." << std::endl;
    std::cout << "See detail in :" << outfile << std::endl;
  }

  std::string get_container_path(int container_id) {
    return root_path + "/merged/partion_" +
           std::to_string(container_id / 1000) + "/" +
           std::to_string(container_id) + ".csv";
  }
  std::string get_container_save_path(int container_id) {
    return root_path + "/sorted/partion_" +
           std::to_string(container_id / 1000) + "/" +
           std::to_string(container_id) + ".csv";
  }

  // delete the sorted file, return true if success or false for failure
  // for test
  bool delete_file(int container_id) {
    std::string save_file = get_container_save_path(container_id);
    int flag = std::remove(save_file.c_str());
    return flag == 0;
  }

  std::vector<std::string> split_str(std::string str, std::string sep = ",") {
    std::vector<std::string> vec;
    if (str.empty())
      return vec;
    int i = 0;
    int j = 0;
    while (j = str.find(sep, i), j != std::string::npos) {
      vec.push_back(str.substr(i, j - i));
      i = j + 1;
    }
    vec.push_back(str.substr(i));
    return vec;
  }
};
