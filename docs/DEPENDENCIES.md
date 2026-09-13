# ChaosProxy + PhotoTask 依赖基线

## 设计基线

- vcpkg 仓库：`https://github.com/microsoft/vcpkg.git`
- vcpkg 固定 commit：`9e593bb18ea69cc5095e012465dcd675a822ed0d`
- builtin baseline：`9e593bb18ea69cc5095e012465dcd675a822ed0d`
- target/host triplet：`x64-linux`
- C++：C++20
- 默认编译器：`/usr/bin/gcc-13`、`/usr/bin/g++-13`
- 工程最低 CMake：3.25
- 默认构建生成器：Ninja

## 当前阶段（A0.1）

当前只提交 Manifest、CMake Presets 和 vcpkg 子模块；尚未声明生产 target、测试 target 或业务源码。`proxy-debug` 是本阶段的首选配置入口。

每个 preset 使用独立的 `build/<preset>/` 与 `vcpkg_installed/`，避免切换 feature 污染已有构建目录。`photo` 与 `kafka` feature 已在 manifest 中固定，但对应 target 会在后续阶段逐步加入。

## 初始化命令（Ubuntu）

```bash
git submodule update --init --recursive
./third_party/vcpkg/bootstrap-vcpkg.sh -disableMetrics
cmake --preset proxy-debug
```

完成 A0.5 后再执行：

```bash
cmake --build --preset proxy-debug
ctest --preset proxy-debug --output-on-failure
```

## 版本记录规则

实际执行 bootstrap、configure、build 或 test 后，将命令和真实输出摘要写入 `docs/state.md`；未执行的步骤不得记录为通过。依赖版本以 `vcpkg.json` 的 baseline/overrides 和子模块 gitlink 为准，不把系统安装的同名库当作项目锁定版本。
