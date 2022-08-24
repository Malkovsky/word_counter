cmake_minimum_required(VERSION 3.22)
project(words_counter)

set(CMAKE_CXX_STANDARD 17)

add_executable(words_counter main.cpp third_party/thread_pool.h)
