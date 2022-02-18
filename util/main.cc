#include "csv2.hpp"
#include <iostream>
#include <map>
#include <fstream>
#include <string>
#include <vector>
#include <mutex>
#include <thread>
using namespace csv2;

std::mutex m;
int cur_dir = 0;
int dir_end = 72;

// store hash [container_id, cou_request]
std::map<int, int> meta_hash;

bool resolve_meta(std::string container_meta = "./container_meta.csv")
{
    Reader<delimiter<','>,
           quote_character<'"'>,
           first_row_is_header<false>,
           trim_policy::trim_whitespace>
        csv;

    std::ifstream in(container_meta);
    if(!in.is_open()) {
        return false;
    }
    in.close();
    // read container_meta.csv
    if (csv.mmap(container_meta))
    {
        for (const auto row : csv)
        {
            int i = 0;
            std::string container_id, cpu_request;
            for (const auto cell : row)
            {
                std::string value;
                cell.read_value(value);
                if (i == 0)
                    container_id = value.substr(2);
                else if (i == 5)
                {
                    cpu_request = value;
                    if (!container_id.empty() and !cpu_request.empty())
                        meta_hash[std::stoi(container_id)] = std::stoi(cpu_request);
                }
                i++;
            }
        }
        return true;
    }
    return false;
}

void zero_file(std::string filename, int cpu_request, std::string savefilename)
{
    int start = 86400;
    int end = 777600;
    int step = 10;
    std::vector<int> cpu_util_percents((end - start) / step, 0);

    std::ifstream in(filename);
    if (in.is_open())
    {
        for (std::string line; std::getline(in, line);)
        {
            auto pos = line.find(',');
            int time_stamp = std::stoi(line.substr(0, pos));
            if(line.substr(pos + 1).empty()) continue;
	    int cpu_util_percent = std::stoi(line.substr(pos + 1));
            int index = (time_stamp - start) / step;
            cpu_util_percents[index] = cpu_util_percent * cpu_request / 96;
        }
        in.close();
        std::ofstream out(savefilename);
        if (out.is_open())
        {
            for (auto ele : cpu_util_percents)
            {
                out << ele << "\n";
            }
            out.close();
        }
    }
}

void zero_dir(int index, std::string dirname, std::string savedirname)
{
    int max_num = 1000;
    for (int i = 0; i < max_num; ++i)
    {
        int container_id = index * max_num + i;
        if (meta_hash.count(container_id) == 0)
            continue;
        int cpu_request = meta_hash[container_id];
        std::string filename = dirname + "/partion_" +
                               std::to_string(index) + "/" + std::to_string(container_id) + ".csv";
        std::string savefilename = savedirname + "/instanceid_" +
                                   std::to_string(container_id) + ".csv";
        zero_file(filename, cpu_request, savefilename);
    }
}

void zero_hole(std::string dir, std::string savedir)
{
    while (true)
    {
        m.lock();
        if (cur_dir >= dir_end)
        {
            m.unlock();
            break;
        }
        int myindex = cur_dir;
        cur_dir++;
        m.unlock();

        zero_dir(myindex, dir, savedir);
    }
}

int main()
{
    std::string container_meta = "./container_meta.csv";
    bool is_resolve = resolve_meta(container_meta);
    if(!is_resolve) {
        std::cout << "Failed to resolve meta csv. " << std::endl;
    }
    std::string dirname = "./sorted";
    std::string savedirname = "./zero_hole";
    int thread_num = 12;
    std::vector<std::thread> threads;
    for(int i = 0; i < thread_num; ++i) {
        threads.push_back(std::thread(zero_hole, dirname, savedirname));
    }
    for(int i = 0; i < thread_num; ++i) {
        threads[i].join();
    }
}
