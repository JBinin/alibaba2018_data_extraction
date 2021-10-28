#include "reader.hpp"
#include "gtest/gtest.h"
#include <fstream>
#include <string>

// test the fucntion of Csv_Reader
// read a data of a container and save it
TEST(Csv_ReaderTest, init_csv_fiel) {
  std::string file_path = "../../test/data/data1.csv";
  Csv_Reader csv_reader;
  bool init_success = csv_reader.initfile(file_path);
  EXPECT_EQ(true, init_success);
}

TEST(Csv_ReaderTest, read) {
  std::string file_path = "../../test/data/data1.csv";
  Csv_Reader csv_reader;
  bool init_success = csv_reader.initfile(file_path);

  int next_container_id = csv_reader.get_next_containerid();
  EXPECT_EQ(next_container_id, 10003);

  csv_reader.reader_container(next_container_id);

  EXPECT_EQ(csv_reader.vec_time_stamp[0], std::string("220740"));
  EXPECT_EQ(csv_reader.vec_time_stamp[1], std::string("220750"));
  EXPECT_EQ(csv_reader.vec_cpu_util_percent[0], std::string("20"));
}

TEST(Csv_ReaderTest, write) {
  std::string file_path = "../../test/data/data1.csv";
  Csv_Reader csv_reader;
  bool init_success = csv_reader.initfile(file_path);

  int next_container_id = csv_reader.get_next_containerid();
  EXPECT_EQ(next_container_id, 10003);

  csv_reader.reader_container(next_container_id);

  std::string save_file;
  save_file = csv_reader.save_file_path(next_container_id);

  std::string expect_save_file = "./partion_10/10003.csv";
  EXPECT_EQ(expect_save_file, save_file);
  csv_reader.writer_container(next_container_id);

  std::ifstream infile(save_file);
  EXPECT_TRUE(infile.is_open());

  std::string time_stamp, cpu_util_percent;
  infile >> time_stamp >> cpu_util_percent;
  infile.close();
  EXPECT_EQ(time_stamp, std::string("220740,220750"));
  EXPECT_EQ(cpu_util_percent, std::string("20,20"));

  // after test: delete the file which produced by test
  csv_reader.delete_file(next_container_id);
}
