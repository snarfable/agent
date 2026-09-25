add_executable(buffer_bench buffer_bench.cpp)
target_compile_features(buffer_bench PRIVATE cxx_std_17)
target_link_libraries(buffer_bench PRIVATE agent)
