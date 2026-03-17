# 快速入门<a name="ZH-CN_TOPIC_0000002520772070"></a>

## 编译构建<a name="ZH-CN_TOPIC_0000002520932058"></a>

### 1. 编译依赖

|硬件依赖||
|--|:-:|
|CPU|Kunpeng-920 / Kunpeng-920B|
|Architecture|aarch64|
|内存|32GB及以上|

|软件依赖|软件版本|
|--|:-:|
|操作系统|Kunpeng-920 / Kunpeng-920B|
|CMake|3.22.0|
|GCC|10.3.1|
|JDK|1.8.0_432|

### 2. 源码编译
1. 下载源代码：
从OpenEuler开源社区下载OmniStateStore的源代码到编译服务器上；
2. 执行编译命令，以编译release包为例：

    ```
    bash scripts/build.sh -t release
    ```
    其它编译选项如下表所示，不同的编译选项可以组合使用。
    | 编译参数  | 编译选项  | 简要说明  |
    | ------------ | ------------ | ------------ |
    | -t  | debug/release  | 编译debug/release包  |
    | --ut  | -  | 编译UT测试程序  |
    | --sve  | -  | 使能鲲鹏高性能SVE指令  |
    | -h  | -  | 帮助  |
3. 检查编译成功的软件包。
编译成功则在目录dist/下存在：
OmniStateStore软件包BoostKit-omnistatestore_1.x.x_aarch64_xxx.tar.gz。

### 3. 开发者测试
1. 执行测试运行脚本。

    ```
    sh test/run_dt.sh
    ```
2. 执行测试运行脚本后会自动编译和测试用例执行，最后观测测试用例执行结果即可。

## 环境部署<a name="ZH-CN_TOPIC_0000002520932058"></a>

环境部署参考以下链接 [installation_guide.md](installation_guide.md)

## 测试验证<a name="ZH-CN_TOPIC_0000002520932058"></a>

1. 进入Flink安装目录下的bin目录，并启动Flink。

    ```
    cd $FLINK_HOME/bin/ && ./start-cluster.sh
    ```

2. 调用sql-client后，进行测试。

    ```
    ./sql-client.sh
    ```

3. 在命令行中输入。

    ```
    SELECT 'Hello, Flink!';
    ```
    可以正常输出结果即安装正常。



