#include "reader.hpp"
#include "resort.hpp"
#include "gtest/gtest.h"
#include <fstream>
#include <string>

// test the fucntion of Csv_Reader
// read a data of a container and save it
TEST(Csv_ReaderTest, init_csv_fiel) {
  std::string file_path = "../../test_data/data1.csv";
  Csv_Reader csv_reader;
  bool init_success = csv_reader.initfile("", file_path);
  EXPECT_EQ(true, init_success);
}

TEST(Csv_ReaderTest, read) {
  std::string file_path = "../../test_data/data1.csv";
  Csv_Reader csv_reader;
  bool init_success = csv_reader.initfile("../../test_data", file_path);

  int next_container_id = csv_reader.get_next_containerid();
  EXPECT_EQ(next_container_id, 10003);

  csv_reader.reader_container(next_container_id);

  EXPECT_EQ(csv_reader.vec_time_stamp[0], std::string("220740"));
  EXPECT_EQ(csv_reader.vec_time_stamp[1], std::string("220750"));
  EXPECT_EQ(csv_reader.vec_cpu_util_percent[0], std::string("20"));
}

TEST(Csv_ReaderTest, write) {
  std::string file_path = "../../test_data/data1.csv";
  Csv_Reader csv_reader;
  bool init_success = csv_reader.initfile("../../test_data", file_path);
  csv_reader.read_and_merge();

  std::string expect_save_file = "../../test_data/merged/partion_10/10003.csv";

  std::ifstream infile(expect_save_file);
  EXPECT_TRUE(infile.is_open());

  std::string time_stamp, cpu_util_percent;
  infile >> time_stamp >> cpu_util_percent;
  infile.close();
  EXPECT_EQ(time_stamp, std::string("220740,220750"));
  EXPECT_EQ(cpu_util_percent, std::string("20,20"));
  // after test: delete the file which produced by test
  int next_container_id = 10003;
  EXPECT_TRUE(csv_reader.delete_file(next_container_id));
}

TEST(LoserTreeTest, merge_sort) {
  std::vector<std::vector<std::string>> time_stamps = {
      {"1", "3", "5"}, {"2", "4", "8"}, {"6", "9", "10"}, {"7", "11", "12"}};

  std::vector<std::vector<std::string>> cpu_util_percents = {
      {"10", "30", "50"},
      {"20", "40", "80"},
      {"60", "90", "100"},
      {"70", "110", "120"}};
  LoserTree lt(time_stamps, cpu_util_percents);

  std::vector<std::string> vec;
  lt.merge_sort(vec);
  EXPECT_EQ(time_stamps.size() * time_stamps[0].size(), vec.size());
  for (int i = 0; i < vec.size(); ++i) {
    std::string expect_str =
        std::to_string(i + 1) + "," + std::to_string((i + 1) * 10);
    EXPECT_EQ(vec[i], expect_str);
  }
}

TEST(ResortTest, split_str) {
  std::string str = "1,2,3,4,5,6";
  auto vec = Resort("./data").split_str(str, ",");
  for (int i = 0; i < vec.size(); ++i) {
    EXPECT_EQ(vec[i], std::to_string(i + 1));
  }
  EXPECT_EQ(vec.size(), 6);
}

TEST(ResortTest, get_path) {
  Resort rsort("./data");
  int container_id = 10003;
  std::string expect_file_name = "./data/merged/partion_10/10003.csv";
  EXPECT_EQ(expect_file_name, rsort.get_container_path(container_id));
  std::string expect_save_name = "./data/sorted/partion_10/10003.csv";
  EXPECT_EQ(expect_save_name, rsort.get_container_save_path(container_id));
}

TEST(ResortTest, sort_one_file) {
  Resort rsort("../../test_data");
  int container_id = 10004;
  EXPECT_EQ("../../test_data/merged/partion_10/10004.csv",
            rsort.get_container_path(container_id));
  rsort.sort_one_file(container_id);
  std::string save_path = rsort.get_container_save_path(container_id);
  std::ifstream infile(save_path);
  EXPECT_EQ(true, infile.is_open()) << save_path << std::endl;
  std::string line;
  for (int i = 1; i <= 15; ++i) {
    infile >> line;
    std::string time_stamp = std::to_string(i);
    std::string cpu_util_percent = std::to_string(i * 10);
    EXPECT_EQ(line, time_stamp + "," + cpu_util_percent);
  }
  infile.close();
}

TEST(ResortTest, sort_files) {
  int size = 3;
  int index = 10004;

  Resort rsort("../../test_data", index - 1000, index + size);
  rsort.sort(1);

  for (int i = 0; i < size; ++i) {
    std::string file_path = rsort.get_container_save_path(index + i);
    std::ifstream infile(file_path);
    EXPECT_TRUE(infile.is_open());
    std::string line;
    for (int j = 1; j <= 15; ++j) {
      infile >> line;
      std::string time_stamp = std::to_string(j);
      std::string cpu_util_percent = std::to_string(j * 10);
      EXPECT_EQ(line, time_stamp + "," + cpu_util_percent);
    }
    infile.close();
  }
  for (int i = 0; i < size; ++i)
    EXPECT_TRUE(rsort.delete_file(index + i));
}
