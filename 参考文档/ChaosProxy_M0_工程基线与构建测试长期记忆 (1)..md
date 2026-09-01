# ChaosProxy M0：工程基线与构建测试长期记忆

> 文档定位：记录 ChaosProxy 阶段 0 / M0 的工程结构、构建配置、完整代码、测试方法、设计理由和验收状态。后续忘记上下文时，可以根据本文从零恢复 M0 工程。

---

## 1. 当前项目位置

```text
项目：ChaosProxy
完成层级：L0 原理闭环版
当前阶段：阶段0——项目地图、验收标准与环境基线
当前里程碑：M0 工程基线
M0状态：代码与测试验收通过
下一阶段：阶段1——阻塞式单连接 TCP 代理闭环
```

### M0 已完成

- 明确 ChaosProxy 是位于客户端与真实服务端之间的 TCP 故障注入代理；
- 明确一个代理会话包含两条 TCP 连接和两个转发方向；
- 明确 TCP 是没有消息边界的有序字节流；
- 建立 C++17、CMake、Ninja、GoogleTest、CTest 工程基线；
- 建立 `chaosproxy_core`、`chaosproxy` 和 `chaosproxy_tests` 三个目标；
- 实现最小命令行入口；
- 验证无参数、`--help` 和非法参数三条路径；
- 三个自动化测试全部通过。

### M0 明确不做

- 不创建监听 Socket；
- 不连接上游服务；
- 不实现 TCP 数据转发；
- 不实现非阻塞 I/O 和 epoll；
- 不实现故障注入策略；
- 不为了展示技术栈加入 Redis、MySQL、Kafka 或线程池。

M0 的唯一作用是提供一个稳定、可编译、可测试的项目起点。

---

## 2. M0 目录结构

```text
chaosproxy/
├── CMakeLists.txt
├── .gitignore
├── README.md
├── REQUIREMENTS.md
├── DESIGN.md
├── PLAN.md
├── STATUS.md
├── DECISIONS.md
├── BUG_LEDGER.md
├── include/
│   └── chaosproxy/
│       └── app.h
├── src/
│   ├── app.cpp
│   └── main.cpp
├── tests/
│   └── unit/
│       └── app_test.cpp
└── build/                  # 构建产物，不提交到 Git
```

### 文件职责

| 文件或目录 | 当前职责 |
|---|---|
| `CMakeLists.txt` | 定义语言标准、编译目标、依赖和测试 |
| `include/chaosproxy/` | 对外公开头文件 |
| `src/app.cpp` | 可复用、可测试的程序核心入口逻辑 |
| `src/main.cpp` | 操作系统进程入口，只调用核心逻辑 |
| `tests/unit/` | 核心模块单元测试 |
| `README.md` | 项目简介、构建和运行方法 |
| `REQUIREMENTS.md` | 第一版必须实现和明确不做的内容 |
| `DESIGN.md` | 数据路径、对象关系、状态机和设计取舍 |
| `PLAN.md` | M0～M7 的实现顺序和验收标准 |
| `STATUS.md` | 当前真正完成到哪里、测试状态和下一步 |
| `DECISIONS.md` | 影响网络语义和架构的重要决策 |
| `BUG_LEDGER.md` | 重要 Bug 的症状、根因、修复和回归测试 |
| `build/` | CMake/Ninja 生成文件、目标文件和可执行程序 |

### `.gitignore`

```gitignore
/build/
/build-*/
```

构建产物必须与源码分离，不能把 CMake 缓存、`.o` 文件和可执行程序提交到 Git。

---

## 3. CMake 目标关系

```text
src/main.cpp
    │
    ▼
chaosproxy 可执行程序
    │ 链接
    ▼
chaosproxy_core 静态/默认库
    ▲
    │ 链接
chaosproxy_tests 测试程序
    ▲
    │
tests/unit/app_test.cpp + GTest::gtest_main
```

### 三个目标分别解决什么

| 目标 | 类型 | 作用 |
|---|---|---|
| `chaosproxy_core` | 库 | 保存可复用、可单元测试的核心代码 |
| `chaosproxy` | 可执行程序 | 提供真实的 `main()` 进程入口 |
| `chaosproxy_tests` | 测试可执行程序 | 调用与正式程序相同的核心实现并执行断言 |

不能把全部逻辑堆进 `main.cpp`。`main()` 只负责把操作系统提供的参数和标准输出流交给核心层；后续 Socket、EventLoop、Connection 等逻辑都应进入可测试模块。

---

## 4. 完整 `CMakeLists.txt`

```cmake
cmake_minimum_required(VERSION 3.16)

project(
    ChaosProxy
    VERSION 0.1.0
    LANGUAGES CXX
)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_library(chaosproxy_core
    src/app.cpp
)

target_include_directories(chaosproxy_core
    PUBLIC
        ${PROJECT_SOURCE_DIR}/include
)

target_compile_options(chaosproxy_core
    PRIVATE
        -Wall
        -Wextra
        -Wpedantic
)

add_executable(chaosproxy
    src/main.cpp
)

target_link_libraries(chaosproxy
    PRIVATE
        chaosproxy_core
)

include(CTest)

if(BUILD_TESTING)
    find_package(GTest REQUIRED)

    add_executable(chaosproxy_tests
        tests/unit/app_test.cpp
    )

    target_link_libraries(chaosproxy_tests
        PRIVATE
            chaosproxy_core
            GTest::gtest_main
    )

    include(GoogleTest)
    gtest_discover_tests(chaosproxy_tests)
endif()
```

### 关键配置解释

#### C++17

```cmake
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
```

- 使用 C++17；
- 编译器不支持 C++17 时配置失败；
- 使用标准 `-std=c++17`，不依赖 `gnu++17` 扩展。

#### 核心库

```cmake
add_library(chaosproxy_core src/app.cpp)
```

正式程序和测试链接同一个 `chaosproxy_core`，保证测试验证的就是正式程序实际使用的实现，而不是复制代码。

#### 头文件传播

```cmake
target_include_directories(chaosproxy_core PUBLIC ${PROJECT_SOURCE_DIR}/include)
```

使用 `PUBLIC` 是因为核心库自身和链接它的目标都需要 `include/` 目录。

#### 测试开关

```cmake
include(CTest)

if(BUILD_TESTING)
    # 测试目标
endif()
```

`include(CTest)` 创建 `BUILD_TESTING` 选项，默认值为 `ON`。设置 `-DBUILD_TESTING=OFF` 后：

- 不查找 GoogleTest；
- 不构建 `chaosproxy_tests`；
- 不向 CTest 注册测试；
- `chaosproxy_core` 和 `chaosproxy` 仍然构建。

---

## 5. M0 完整代码

### `include/chaosproxy/app.h`

```cpp
#pragma once

#include <iosfwd>

namespace chaosproxy {

int Run(
    int argc,
    char* argv[],
    std::ostream& output,
    std::ostream& error
);

}  // namespace chaosproxy
```

### `src/app.cpp`

```cpp
#include "chaosproxy/app.h"

#include <ostream>
#include <string_view>

namespace chaosproxy {
namespace {

void PrintUsage(std::ostream& output) {
    output
        << "ChaosProxy 0.1.0\n"
        << "Usage: chaosproxy [--help]\n"
        << "\n"
        << "M0 engineering scaffold.\n"
        << "TCP proxy networking is not implemented yet.\n";
}

}  // namespace

int Run(
    int argc,
    char* argv[],
    std::ostream& output,
    std::ostream& error
) {
    if (argc == 1) {
        output << "ChaosProxy M0 scaffold\n";
        return 0;
    }

    const std::string_view argument{argv[1]};

    if (argc == 2 && argument == "--help") {
        PrintUsage(output);
        return 0;
    }

    error << "Unknown or invalid arguments\n";
    return 2;
}

}  // namespace chaosproxy
```

### `src/main.cpp`

```cpp
#include "chaosproxy/app.h"

#include <iostream>

int main(int argc, char* argv[]) {
    return chaosproxy::Run(
        argc,
        argv,
        std::cout,
        std::cerr
    );
}
```

### `tests/unit/app_test.cpp`

```cpp
#include "chaosproxy/app.h"

#include <gtest/gtest.h>

#include <sstream>
#include <string>

namespace {

TEST(AppTest, RunsWithoutArguments) {
    char program_name[] = "chaosproxy";
    char* argv[] = {program_name};

    std::ostringstream output;
    std::ostringstream error;

    const int result = chaosproxy::Run(1, argv, output, error);

    EXPECT_EQ(result, 0);
    EXPECT_NE(output.str().find("ChaosProxy"), std::string::npos);
    EXPECT_TRUE(error.str().empty());
}

TEST(AppTest, PrintsHelp) {
    char program_name[] = "chaosproxy";
    char help_argument[] = "--help";
    char* argv[] = {program_name, help_argument};

    std::ostringstream output;
    std::ostringstream error;

    const int result = chaosproxy::Run(2, argv, output, error);

    EXPECT_EQ(result, 0);
    EXPECT_NE(output.str().find("Usage:"), std::string::npos);
    EXPECT_TRUE(error.str().empty());
}

TEST(AppTest, RejectsUnknownArgument) {
    char program_name[] = "chaosproxy";
    char unknown_argument[] = "--unknown";
    char* argv[] = {program_name, unknown_argument};

    std::ostringstream output;
    std::ostringstream error;

    const int result = chaosproxy::Run(2, argv, output, error);

    EXPECT_NE(result, 0);
    EXPECT_TRUE(output.str().empty());
    EXPECT_NE(error.str().find("Unknown"), std::string::npos);
}

}  // namespace
```

---

## 6. 为什么 `Run` 接收输出流

接口：

```cpp
int Run(
    int argc,
    char* argv[],
    std::ostream& output,
    std::ostream& error
);
```

真实运行时：

```cpp
Run(argc, argv, std::cout, std::cerr);
```

此时数据进入终端。

测试时：

```cpp
std::ostringstream output;
std::ostringstream error;

Run(argc, argv, output, error);
```

此时数据进入 `std::ostringstream` 的内存缓冲区，可以通过：

```cpp
output.str();
error.str();
```

取出并断言。

核心思想：

```text
Run不把输出位置写死
→ 调用者传入输出目的地
→ 正式程序写终端
→ 测试写内存并检查内容
```

`Run` 只是借用 `std::ostream&`，不拥有也不销毁外部流对象。

---

## 7. CMake、Ninja、g++、GTest、CTest 的关系

### 构建链路

```text
CMakeLists.txt
→ CMake分析目标与依赖
→ 生成build.ninja
→ Ninja调度需要执行的构建任务
→ g++编译.cpp并链接库/可执行程序
```

| 工具 | 职责 |
|---|---|
| CMake | 描述和生成构建系统 |
| Ninja | 执行构建计划、处理增量和并行构建 |
| g++ | 真正编译与链接 C++ 代码 |

### 测试链路

```text
app_test.cpp使用GTest编写测试
→ 编译得到chaosproxy_tests
→ gtest_discover_tests发现各个TEST
→ CTest启动测试并汇总结果
```

| 工具 | 职责 |
|---|---|
| GoogleTest / GTest | 提供 `TEST`、`EXPECT_EQ`、`EXPECT_TRUE` 等 C++ 测试能力 |
| CTest | 统一注册、启动和汇总项目测试 |

记忆方式：

```text
GTest决定测试里面怎么检查
CTest决定项目有哪些测试以及怎样统一运行
```

---

## 8. 构建参数

### `-G Ninja`

```bash
cmake -S . -B build -G Ninja
```

表示让 CMake 生成 Ninja 使用的 `build.ninja`。之后：

```bash
cmake --build build
```

会间接调用 Ninja。

不使用 Ninja 时，可以使用单独的构建目录并采用默认生成器：

```bash
cmake -S . -B build-make
cmake --build build-make
```

不能在同一个构建目录中随意切换生成器。

### `-DCMAKE_BUILD_TYPE=Debug`

```bash
-DCMAKE_BUILD_TYPE=Debug
```

为 CMake 设置 Debug 构建类型，通常包含调试符号并减少优化，方便使用 GDB 和定位问题。

常见构建类型：

| 类型 | 用途 |
|---|---|
| `Debug` | 日常开发、调试、单元测试 |
| `Release` | 正式性能测试 |
| `RelWithDebInfo` | 优化构建，同时保留调试符号 |

建议后续分开使用：

```text
build-debug/
build-release/
```

### `-DBUILD_TESTING`

默认：

```text
BUILD_TESTING=ON
```

关闭测试：

```bash
cmake -S . -B build-release \
    -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_TESTING=OFF
```

查看当前配置：

```bash
cmake -LA -N build | grep -E 'BUILD_TESTING|CMAKE_BUILD_TYPE'
```

---

## 9. 环境与构建命令

### 安装依赖（Debian/Ubuntu）

```bash
sudo apt update
sudo apt install -y g++ cmake ninja-build libgtest-dev
```

### 配置

```bash
cmake \
    -S . \
    -B build \
    -G Ninja \
    -DCMAKE_BUILD_TYPE=Debug
```

其中每行末尾的 `\` 是 Bash 续行符，表示下一行仍属于同一条命令。

### 编译

```bash
cmake --build build -j
```

### 执行全部测试

```bash
ctest \
    --test-dir build \
    --output-on-failure
```

### 直接运行 GTest 测试程序

```bash
./build/chaosproxy_tests
```

### 运行程序

```bash
./build/chaosproxy
```

预期：

```text
ChaosProxy M0 scaffold
```

### 查看帮助

```bash
./build/chaosproxy --help
```

预期：

```text
ChaosProxy 0.1.0
Usage: chaosproxy [--help]

M0 engineering scaffold.
TCP proxy networking is not implemented yet.
```

### 非法参数测试

```bash
./build/chaosproxy --unknown
echo $?
```

预期：

```text
Unknown or invalid arguments
2
```

`$?` 保存上一条命令的退出码，因此必须紧跟在要检查的命令之后执行。

---

## 10. 已验证结果

用户实际环境中的验证结果：

```text
C++编译器：GNU 15.2.0
GoogleTest：1.17.0
CMake配置：成功
Ninja构建：成功
CTest结果：3/3通过
程序无参数运行：成功
--help：输出正确
--unknown：输出错误信息，退出码为2
```

已通过测试：

```text
AppTest.RunsWithoutArguments
AppTest.PrintsHelp
AppTest.RejectsUnknownArgument
```

---

## 11. M0 设计决定

### D-M0-001：核心逻辑与 `main()` 分离

```text
选择：核心逻辑放入chaosproxy_core，main.cpp只保留入口调用。
原因：正式程序和测试能够复用同一份实现，降低进程级测试成本。
边界：后续业务逻辑不得重新堆入main.cpp。
```

### D-M0-002：使用 GoogleTest + CTest

```text
选择：GTest负责C++断言，CTest负责项目级测试运行与汇总。
原因：既能方便编写单元测试，也能用统一命令执行全部测试。
```

### D-M0-003：开发阶段使用 Ninja + Debug

```text
选择：M0使用Ninja生成器和Debug构建。
原因：获得快速增量构建和调试信息。
重新评估：性能压测阶段增加独立Release构建目录。
```

### D-M0-004：测试通过输出流注入捕获结果

```text
选择：Run接收std::ostream&，而不是内部写死std::cout/std::cerr。
原因：真实运行可以写终端，单元测试可以写入ostringstream并断言。
```

---

## 12. M0 不变量

1. `main.cpp` 只负责进程入口，核心逻辑进入可测试模块；
2. 正式程序和测试必须链接同一份 `chaosproxy_core` 实现；
3. M0 输出必须明确网络代理尚未实现；
4. 测试至少覆盖一条正常路径和一条错误路径；
5. 构建产物必须与源码分离；
6. 未通过当前里程碑验收前，不叠加未来网络功能。

---

## 13. M0 验收标准

```text
[x] CMake可以配置
[x] Ninja可以构建
[x] chaosproxy_core可以生成
[x] chaosproxy可执行程序可以生成
[x] chaosproxy_tests可以生成
[x] CTest能够发现三个GTest用例
[x] 三个测试全部通过
[x] 无参数运行成功
[x] --help输出成功
[x] 非法参数返回非零退出码
[x] 能解释CMake、Ninja和g++的职责
[x] 能解释GTest与CTest的区别
[x] 能解释std::ostream&输出流注入
```

如果工程文档骨架均已建立，则 M0 正式完成。

---

## 14. 下一阶段入口

下一阶段：

```text
阶段1：阻塞式单连接 TCP 代理闭环
里程碑：M1 最小透明代理
```

下一阶段只解决：

```text
如何让一个客户端通过ChaosProxy与固定Echo Server完成双向字节转发。
```

核心知识原子：

1. 创建、绑定和监听 Socket；
2. `accept` 得到 `client_fd`；
3. 创建 `upstream_fd` 并连接固定上游；
4. 两个方向为什么必须同时转发；
5. 阻塞模型为什么不能用一个简单循环同时等待两个方向；
6. 使用两个线程或 `poll` 建立最小双向闭环；
7. 验证字节一致性、正常 EOF 和基本错误退出。

M1 完成条件：

```text
客户端 → ChaosProxy → Echo Server → ChaosProxy → 客户端
```

整条数据路径能够工作，且输入输出逐字节一致。

---

## 15. 长期记忆摘要

```text
M0不是代理功能阶段，而是工程基线阶段。

CMake描述目标和依赖；
Ninja执行构建计划；
g++真正编译和链接；
GTest负责测试断言；
CTest负责统一运行测试。

chaosproxy_core保存核心实现；
chaosproxy提供真实main入口；
chaosproxy_tests复用同一核心实现。

Run接收std::ostream&：
真实运行传cout/cerr，测试传ostringstream。

M0已经通过配置、编译、运行和3个自动化测试。
下一步进入阻塞式单连接TCP代理闭环。
```
