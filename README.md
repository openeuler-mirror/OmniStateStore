# 项目介绍
#### 背景
随着大型计算系统的出现，不同领域的组织以实时方式生成大量数据，Flink是一种开源的分布式计算框架，专为处理大规模数据流而设计，它能够在低时延和高吞吐的场景下高效地处理实时数据。后端状态存储系统在其中扮演着重要角色，当作业状态规模较大时，状态存储系统无法将全部的状态数据存储在内存中，往往会将冷数据存储在大容量磁盘上，但是内存和磁盘两种介质在访问性能上存在巨大的差异，IO读写很容易成为数据处理的瓶颈，比如在计算过程中如果某个算子需要频繁从磁盘上加载状态数据，那么这个算子就很容易成为整个作业吞吐的性能瓶颈，因此后端状态存储系统在很多时候是决定Flink作业性能的决定性因素。
目前开源Flink采用的状态后端包括MemoryStateBackend、FsStateBackend和RocksDBStateBackend，每种stateBackend都有其优缺点，适用于不同的场景。MemoryStateBackend在大规模状态管理中，由于内存限制可能会导致OOM（Out of Memory）错误，并且在大规模任务中从内存中恢复状态速度慢。FsStateBackend依赖于外部存储系统的I/O性能，在高负载或网络延迟高的情况下状态访问速度可能变慢，因此RocksDBStateBackend是当前使用最为广泛的后端存储系统，但是RocksDB作为一个通用的KV存储引擎，并不完全适合流式计算场景，在实际生产使用中会遭遇以下问题：
1. RocksDB是一个本地嵌入式数据库，状态访问IO性能差，会消耗大量CPU资源；
2. 基于LSM-Tree实现的文件持久化存储中数据顺序批量读写磁盘，查询延迟较大，拖慢整体流计算吞吐率；
3. 扩缩并发场景下状态恢复较慢。
并且随着互联网搜推应用快速发展，产生的状态数据量也日益增多，大规模作业下甚至会产生十几~几十TB的状态数据量，那么RocksDBStateBackend的局限性会被无限放大。因此我们针对生产环境中遇到的问题，再结合实际应用，推出了高性能状态存储引擎OmniStateStore，旨在解决实际生产使用中遇到的痛点或瓶颈点，更好地适用于大规模Flink作业场景。

#### OmniStateStore介绍
状态存储引擎OmniStateStore整体架构上采用以磁盘为主要存储，内存作为Cache的方案，软件架构视图从上到下可分为FreshTable、SliceTable和文件存储LsmStore模块，其核心优势具有：
1. FreshTable模块能够快速终结应用IO，其状态数据采用紧密内存结构的哈希索引，相比传统排序索引性能提升显著；
2. SliceTable模块中的状态数据重新根据state key进行hash打散，控制单个SliceDate数据，结合鲲鹏L1 Cache优异特性，具有更好地状态数据查询性能；
3. 重新设计LSM-Tree中的文件数据，改善磁盘存储结构，优化大状态场景下的状态数据在文件层的读写性能；
4. 提供状态数据懒加载功能，能够大幅提升启动和并行度变更场景下的状态恢复性能。

#### 使能场景
1. OmniStateStore是基于开源Flink的状态存储后端标准接口实现，未对开源Flink本身做任何侵入式修改，因此支持以Jar包形式平滑替换当前开源Flink支持的任一种StateBackend。
2. 当前开源Flink广泛应用的状态后端是RocksDBStateBackend，OmniStateStore平滑替换后可以带来端到端的性能提升，其性能提升比例取决于状态后端在整个Flink作业期间的CPU占比，CPU占比越高性能提升越明显。

#### 版本说明
1. 仅支持在华为鲲鹏算力平台上运行，支持鲲鹏920、920B等服务器类型；
2. 适用于Flink 1.16.1、1.16.3和1.17.1三个版本的状态后端；
3. 支持容器规格最小存算比为1:2。

# 快速上手
#### 编译依赖
1. 软件依赖：
- OS：openEuler20.03、openEuler22.03、openEuler24.03
- cmake：3.22.0
- GCC：10.3.1
- JDK：1.8.0_432

#### 源码编译
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

#### 开发者测试
1. 执行测试运行脚本。

```
sh test/run_dt.sh
```
2. 执行测试运行脚本后会自动编译和测试用例执行，最后观测测试用例执行结果即可。

# 安装部署
#### 软件安装
1. 登录待安装节点，上传软件包BoostKit-omnistatestore_1.x.x_aarch64_xxx.tar.gz到“${FLINK_HOME}/lib/”子目录下；
2. 解压软件包；

```
tar -zxvf BoostKit-omnistatestore_1.x.x_aarch64_xxx.tar.gz
```
解压后的软件包里包含以下OmniStateStore对应的插件JAR包：
flink-boost-statebackend-1.x.x-SNAPSHOT-for-flink-1.16.1.jar
flink-boost-statebackend-1.x.x-SNAPSHOT-for-flink-1.16.3.jar
flink-boost-statebackend-1.x.x-SNAPSHOT-for-flink-1.17.1.jar
根据具体的Flink版本选择对应的版本JAR包保留即可，删除其他不需要的JAR包。
3. 删除软件压缩包

```
rm -f BoostKit-omnistatestore_1.x.x_aarch64_xxx.tar.gz
```
#### 软件启动
1. 配置参数项；
根据业务使用情况和待安装部署的环境设置Flink的conf子目录下flink-conf.yaml中的相关配置项，OmniStateStore相关配置项说明请参考开发者指南5章节。
2. 启动Flink任务，查看日志中的配置项，检查配置是否成功；
3. 执行“${FLINK_HOME}/examples/streaming/WordCount.jar”示例程序，观察到Task Manager日志中打印“OmniStateStore service start success.”，说明OmniStateStore启动成功。

#### 软件卸载
1. 将配置的state.backend.ockdb.localdir路径删除；
2. 将“${FLINK_HOME}/lib/”目录下的flink-boost-statebackend-1.x.x-SNAPSHOT-for-flink-${flink.version}.jar删除；
3. 将flink-conf.yaml配置文件中的state.backend切换为其他状态后端。

# 贡献指南
如果使用过程中有任何问题，或者需要反馈特性需求和bug报告，可以提交issues联系我们。

# 免责声明
此代码仓参与openeuler软件开源，编码风格遵照原生开源软件，继承原生开源软件安全设计，不破坏原生开源软件设计及编码风格和方式，软件的任何漏洞与安全问题，均由相应的上游社区根据其漏洞和安全响应机制解决。请密切关注上游社区发布的通知和版本更新。鲲鹏计算社区对软件的漏洞及安全问题不承担任何责任。

# 许可证书
木兰宽松许可证（http://license.coscl.org.cn/MulanPSL2）

# 源码开放
敬请期待