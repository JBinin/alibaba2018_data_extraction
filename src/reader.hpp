#include "csv2.hpp"
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <queue>
#include <string>
#include <vector>
using namespace csv2;

class data_type {
public:
  std::string container_id;
  std::string time_stamp;
  std::string cpu_util_percent;
  data_type(std::string container_id, std::string time_stamp,
            std::string cpu_util_percent)
      : container_id(container_id), time_stamp(time_stamp),
        cpu_util_percent(cpu_util_percent) {}
};

class Csv_Reader {
public:
  Reader<delimiter<','>, quote_character<'"'>, first_row_is_header<false>,
         trim_policy::trim_whitespace>
      csv;
  using RowIteratorType =
      Reader<delimiter<','>, quote_character<'"'>, first_row_is_header<false>,
             trim_policy::trim_whitespace>::RowIterator;
  RowIteratorType *p_rows;
  // the path of the usage container csv file
  std::string csvfilename;
  std::string dir;

  std::vector<std::string> vec_time_stamp;
  std::vector<std::string> vec_cpu_util_percent;
  std::queue<data_type> que;

  Csv_Reader() { p_rows = nullptr; }
  ~Csv_Reader() { delete p_rows; }
  bool initfile(std::string data_dir, std::string filename = "") {
    if (data_dir.back() == '/') {
      data_dir = data_dir.substr(0, data_dir.size() - 1);
    }
    dir = data_dir;
    csvfilename = dir + "/container_usage.csv";
    if (!filename.empty())
      csvfilename = filename;
    if (csv.mmap(csvfilename)) {
      p_rows = new RowIteratorType(csv.begin());
      return true;
    }
    return false;
  }

  // read all container data
  // save the same container_id data in same file
  void read_and_merge() {
    int next_container = -1;
    while ((next_container = get_next_containerid()), next_container != -1) {
      reader_container(next_container);
      writer_container(next_container);
    }
  }

  // get the next data's container_id
  // if all data has to be accessed, return -1
  int get_next_containerid() {
    if (que.empty()) {
      read_from_csv();
    }
    if (!que.empty()) {
      data_type data = que.front();
      int container_id = std::stoi(data.container_id.substr(2));
      return container_id;
    }
    return -1;
  }

  void reader_container(int container_id) {
    read_enough_data_of_one_container(container_id);
    std::string container_id_str = "c_" + std::to_string(container_id);
    while (!que.empty()) {
      data_type data = que.front();
      if (data.container_id != container_id_str) {
        break;
      } else {
        vec_time_stamp.push_back(data.time_stamp);
        vec_cpu_util_percent.push_back(data.cpu_util_percent);
        que.pop();
      }
    }
  }

  void writer_container(int container_id) {
    std::string file_path = save_file_path(container_id);

    // if dirctory is not exist, create it
    std::filesystem::path p(file_path);
    std::filesystem::create_directory(p.parent_path());

    std::ofstream stream(file_path, std::ios::app);
    Writer<delimiter<','>> writer(stream);
    writer.write_row(vec_time_stamp);
    writer.write_row(vec_cpu_util_percent);
    stream.close();
    vec_time_stamp.clear();
    vec_cpu_util_percent.clear();
  }

  std::string save_file_path(int container_id) {
    // store 1000 file in one dictory
    int dirctory_size = 1000;
    int index = container_id / dirctory_size;
    std::string save_director =
        dir + "/merged" + "/partion_" + std::to_string(index);
    std::string file_path =
        save_director + "/" + std::to_string(container_id) + ".csv";
    return file_path;
  }

  // delete the result file of container_id, return true if success or false for
  // failure this fucntion is used by test
  bool delete_file(int container_id) {
    std::string file_path = save_file_path(container_id);
    int flag = std::remove(file_path.c_str());
    return flag == 0;
  }

private:
  // read some data from csv file
  // if this is the last read return true else return false
  bool read_from_csv() {
    std::string container_id, machine_id, time_stamp, cpu_util_percent,
        mem_util_percent, cpi, mem_gps, mpki, net_in, net_out, disk_io_percent;

    std::vector<std::string> value(11, std::string());
    // read 1000 lines for one invocation
    int read_lines_num = 1000;
    int index = 0;
    while ((*p_rows) != csv.end()) {
      int i = 0;
      for (const auto cell : (**p_rows)) {
        cell.read_value(value[i]);
        i++;
      }

      ++(*p_rows);

      container_id = value[0];
      machine_id = value[1];
      time_stamp = value[2];
      cpu_util_percent = value[3];
      mem_util_percent = value[4];
      cpi = value[5];
      mem_gps = value[6];
      mpki = value[7];
      net_in = value[8];
      net_out = value[9];
      disk_io_percent = value[10];

      for (int j = 0; j < value.size(); ++j) {
        value[j] = "";
      }

      if (container_id.empty())
        continue;
      data_type data(container_id, time_stamp, cpu_util_percent);
      que.push(data);

      index++;
      if (index >= read_lines_num) {
        return false;
      }
    }
    return true;
  }

  void read_enough_data_of_one_container(int container_id) {
    std::string container_id_str = "c_" + std::to_string(container_id);
    while (que.empty() || que.back().container_id == container_id_str) {
      if (read_from_csv()) {
        break;
      }
    }
  }
};
