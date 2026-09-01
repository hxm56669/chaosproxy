# ChaosProxy

ChaosProxy 是一个面向故障注入实验的 TCP 代理项目。当前版本为 **M0 工程骨架**：命令行入口、核心库和单元测试已经建立，但 Socket、网络转发和故障注入尚未实现。

## 构建

环境要求：

- 支持 C++17 的编译器
- CMake 3.16 或更高版本
- GoogleTest
- Ninja（推荐，也可使用其他 CMake 生成器）

```bash
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j
ctest --test-dir build --output-on-failure
```

如只需构建程序，可关闭测试：

```bash
cmake -S . -B build -DBUILD_TESTING=OFF
```

## 运行

```bash
./build/chaosproxy
./build/chaosproxy --help
```

Windows 使用多配置生成器时，可执行文件通常位于 `build/Debug/chaosproxy.exe`。

