#include "csv2.hpp"
#include <iostream>
#include <map>
#include <fstream>
#include <string>
#include <vector>
#include <mutex>
#include <thread>
#include <iomanip>

using namespace csv2;

std::mutex m;
int cur_dir = 0;
int dir_end = 72;
// store hash [container_id, (cpu_request,mem_size)]
std::map<int, std::pair<int, double>> meta_hash;

bool resolve_meta(std::string container_meta = "./container_meta.csv") {
    Reader<delimiter<','>,
            quote_character<'"'>,
            first_row_is_header<false>,
            trim_policy::trim_whitespace>
            csv;

    std::ifstream in(container_meta);
    if (!in.is_open()) {
        return false;
    }
    in.close();
    // read container_meta.csv
    if (csv.mmap(container_meta)) {
        for (const auto row: csv) {
            int i = 0;
            std::string container_id, cpu_request, mem_size;
            for (const auto cell: row) {
                std::string value;
                cell.read_value(value);
                if (i == 0)
                    container_id = value.substr(2);
                else if (i == 5) {
                    cpu_request = value;
                } else if (i == 7) {
                    mem_size = value;
                    if (!container_id.empty() and !cpu_request.empty() and !mem_size.empty())
                        meta_hash[std::stoi(container_id)] = std::make_pair(std::stoi(cpu_request),
                                                                            std::stod(mem_size));
                }
                i++;
            }
        }
        return true;
    }
    return false;
}

void hole_file(std::string filename, int cpu_request, double mem_size, std::string savefilename) {
    int start = 86400;
    int end = 777600;
    int step = 10;

    std::vector<double> cpu_requests((end - start) / step, 0);
    std::vector<double> mem_sizes((end - start) / step, 0.0);

    std::ifstream in(filename);
    if (in.is_open()) {
        int last_index = -1;
        double last_mem_util = 0.0;
        for (std::string line; std::getline(in, line);) {
            auto pos1 = line.find(',');
            int time_stamp = std::stoi(line.substr(0, pos1));
            int index = (time_stamp - start) / step;

            auto pos2 = line.find(',', pos1 + 1);
            if (!line.substr(pos1 + 1, pos2 - pos1 - 1).empty()) {
                int cpu_util_percent = std::stoi(line.substr(pos1 + 1, pos2 - pos1 - 1));
                if(cpu_util_percent < 0 or cpu_util_percent > 100) {
                  cpu_util_percent = 0;
                } 
                cpu_requests[index] = cpu_util_percent / 100.0 * cpu_request / 96;
            }

            if(!line.substr(pos2 + 1).empty()) {
                int mem_util_percent = std::stoi(line.substr(pos2 + 1));
                if(mem_util_percent < 0 or mem_util_percent > 100) {
                  mem_util_percent = 0;
                }
                mem_sizes[index] = mem_util_percent * mem_size / 100;

                if(last_index + 1 < index and last_index != -1) {
                    double delta = (mem_sizes[index] - last_mem_util) / (index - last_index);
                    while(last_index + 1 < index) {
                        mem_sizes[last_index + 1] = mem_sizes[last_index] + delta;
                        last_index++;
                    }
                }
                last_index = index;
                last_mem_util = mem_sizes[index];
            }
        }
        in.close();
        std::ofstream out(savefilename);
        if (out.is_open()) {
            out << std::fixed << std::setprecision(2);
            for(int i = 0; i <cpu_requests.size(); ++i) {
                if(cpu_requests[i] > 100 or mem_sizes[i] > 100) {
                    std::cout << savefilename << std::endl;
                }
                out << cpu_requests[i] << "," << mem_sizes[i] << "\n";
            }
            out.close();
        }
    }
}

void hole_dir(int index, std::string dirname, std::string savedirname) {
    int max_num = 1000;
    for (int i = 0; i < max_num; ++i) {
        int container_id = index * max_num + i;
        if (meta_hash.count(container_id) == 0)
            continue;
        int cpu_request = meta_hash[container_id].first;
        double mem_size = meta_hash[container_id].second;

        std::string filename = dirname + "/partion_" +
                               std::to_string(index) + "/" + std::to_string(container_id) + ".csv";
        std::string savefilename = savedirname + "/instanceid_" +
                                   std::to_string(container_id) + ".csv";
        hole_file(filename, cpu_request, mem_size, savefilename);
    }
}

void hole(std::string dir, std::string savedir) {
    while (true) {
        m.lock();
        if (cur_dir >= dir_end) {
            m.unlock();
            break;
        }
        int myindex = cur_dir;
        cur_dir++;
        m.unlock();

        hole_dir(myindex, dir, savedir);
    }
}

int main() {
    std::string container_meta = "./container_meta.csv";
    bool is_resolve = resolve_meta(container_meta);
    if (!is_resolve) {
        std::cout << "Failed to resolve meta csv. " << std::endl;
    }
    std::string dirname = "./sorted";
    std::string savedirname = "./hole";
    int thread_num = 12;
    std::vector<std::thread> threads;
    for (int i = 0; i < thread_num; ++i) {
        threads.push_back(std::thread(hole, dirname, savedirname));
    }
    for (int i = 0; i < thread_num; ++i) {
        threads[i].join();
    }
}
