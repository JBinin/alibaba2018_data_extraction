#include "reader.hpp"
#include "resort.hpp"
#include <chrono>
#include <iomanip>
#include <iostream>
#include <string>
int main() {
  auto start = std::chrono::high_resolution_clock::now();

  std::string data_root_path = "../../data";

  Csv_Reader csv_reader;
  csv_reader.initfile(data_root_path);
  csv_reader.read_and_merge();

  Resort resort(data_root_path, 1, 71477, 24);
  resort.sort(10);

  auto end = std::chrono::high_resolution_clock::now();

  resort.output(71477 - 1);

  std::cout
      << std::setprecision(2)
      << std::chrono::duration<double, std::ratio<60>>(end - start).count()
      << " minutes." << std::endl;
  return 0;
}
