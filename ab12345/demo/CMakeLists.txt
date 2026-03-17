add_executable(demo_policy policy.c)
target_link_libraries(demo_policy err)

add_executable(demo_env env.c)
target_link_libraries(demo_env err)