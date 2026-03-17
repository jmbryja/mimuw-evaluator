cmake_minimum_required(VERSION 3.24)

project(evaluator LANGUAGES C CXX)

# set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_C_STANDARD 17)
set(CMAKE_C_STANDARD_REQUIRED ON)

add_compile_options(
    -Werror
    -Wall -Wextra
    -Wno-sign-compare -Wno-unused-parameter -Wno-uninitialized -Wno-unused-function
)

if (CMAKE_BUILD_TYPE STREQUAL "Release")
    add_compile_options(-march=native -O3)
endif()

set(CMAKE_C_CLANG_TIDY "clang-tidy")

# Optionally: include-what-you-use to minimize #include-s.
# find_program(IWYU_PATH NAMES include-what-you-use iwyu)
# if(IWYU_PATH)
#     set(CMAKE_C_INCLUDE_WHAT_YOU_USE ${IWYU_PATH})
# else()
#     message(WARNING "Could not find the program include-what-you-use / iwyu")
# endif()


# Default sizes (fall back to 4) but allow overrides via environment variables
if(DEFINED ENV{STATE_SIZE})
    set(STATE_SIZE $ENV{STATE_SIZE})
else()
    set(STATE_SIZE 4)
endif()

if(DEFINED ENV{ACTION_SIZE})
    set(ACTION_SIZE $ENV{ACTION_SIZE})
else()
    set(ACTION_SIZE 4)
endif()

if(DEFINED ENV{NAME_SIZE})
    set(NAME_SIZE $ENV{NAME_SIZE})
else()
    set(NAME_SIZE 4)
endif()

add_compile_definitions(STATE_SIZE=${STATE_SIZE})
add_compile_definitions(ACTION_SIZE=${ACTION_SIZE})
add_compile_definitions(NAME_SIZE=${NAME_SIZE})

include_directories(${PROJECT_SOURCE_DIR})

add_subdirectory(common)
add_subdirectory(demo)
add_subdirectory(solution)
