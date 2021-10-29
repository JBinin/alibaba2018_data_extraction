#include <algorithm>
#include <fstream>
#include <string>
#include <vector>
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
  Resort(std::string root_path, int thread_count = 24)
      : root_path(root_path), thread_count(thread_count) {
    if (this->root_path.back() == '/') {
      this->root_path = this->root_path.substr(0, this->root_path.size() - 1);
    }
  }
  
  void sort_files() {}
  
  void sort_one_file(int container_id) {
    std::string file_path = get_container_path(container_id);

    std::ifstream infile(file_path);
    if (!infile.is_open()) {
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
    std::ofstream outfile(save_file);
    for(int i = 0; i < result.size(); ++i)
      outfile << result[i]  << std::endl;
    outfile.close();
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
