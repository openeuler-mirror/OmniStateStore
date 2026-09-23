# 快速入门<a name="ZH-CN_TOPIC_0000002520772070"></a>

## 编译构建<a name="ZH-CN_TOPIC_0000002520932058"></a>

### 编译依赖

|硬件依赖|硬件说明|
|--|:-:|
|CPU|Kunpeng-920<br>Kunpeng-920B|
|Architecture|aarch64|
|内存|32GB及以上|

|软件依赖|软件版本|
|--|:-:|
|操作系统|Kunpeng-920<br>Kunpeng-920B|
|CMake|3.22.0|
|GCC|10.3.1|
|JDK|1.8.0_432|

还需安装Maven，并设置JAVA_HOME指向包含JNI头文件的JDK目录。Native单元测试依赖libaio-devel和libasan。可直接使用[容器环境部署](installation_guide.md#容器环境部署)提供的环境镜像，具体内容见[镜像说明](../../docker/README.md)。

### 源码编译

1. 下载源代码.
    从OpenEuler开源社区下载OmniStateStore的源代码到编译服务器上；
2. 执行编译命令，以编译release包为例：

    ```cmd
    bash scripts/build.sh -t release
    ```

    其它编译选项如下表所示，不同的编译选项可以组合使用。

    | 编译参数  | 编译选项  | 简要说明  |
    | ------------ | ------------ | ------------ |
    | -t  | debug/release/blend  | 编译debug、release或blend包  |
    | --fv  | 1.16.1/1.16.3/1.17.1/1.20.0  | 仅编译指定Flink版本  |
    | -j/--jobs  | 正整数  | 设置并行编译任务数，默认读取`BSS_BUILD_JOBS`，未设置时为8  |
    | --ut  | 无  | 编译UT测试程序  |
    | --sve  | 无  | 使能鲲鹏高性能SVE指令  |
    | -h  | 无  | 帮助  |

3. 检查编译成功的软件包。
编译成功则在目录dist/下存在：
OmniStateStore软件包BoostKit-omnistatestore_1.x.x_aarch64_xxx.tar.gz。

### 开发者测试

1. 编译UT测试程序。以下命令使用8个并行编译任务，可通过`-j`、`--jobs`或环境变量`BSS_BUILD_JOBS`调整。

    ```bash
    bash scripts/build.sh -t debug --ut -j 8
    ```

2. 直接执行生成的LLT二进制，并输出GoogleTest XML报告。

    ```bash
    (cd build/test/llt && ./bss_ut --gtest_output=xml:report.xml)
    ```

3. `sh test/run_dt.sh`依赖hdt工具链，可作为已有hdt环境的兼容入口。该入口属于历史辅助流程；在新的系统、编译器或hdt版本上使用前，需要单独验证兼容性。日常开发和CI建议以上述直接构建、直接执行`bss_ut`的结果为准。

## 环境部署<a name="ZH-CN_TOPIC_0000002520932058"></a>

环境部署请参见[安装指南](installation_guide.md)。

## 测试验证<a name="ZH-CN_TOPIC_0000002520932058"></a>

1. 进入Flink安装目录下的bin目录，并启动Flink。

    ```cmd
    cd $FLINK_HOME/bin/ && ./start-cluster.sh
    ```

2. 执行“${FLINK_HOME}/examples/streaming/WordCount.jar”示例程序。

    ```cmd
    $FLINK_HOME/bin/flink run $FLINK_HOME/examples/streaming/WordCount.jar
    ```

    观察到Task Manager日志中打印“OmniStateStore service start success.”，说明OmniStateStore启动成功。
