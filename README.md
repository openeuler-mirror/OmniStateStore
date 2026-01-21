# 项目介绍
#### 背景
随着大数据产业的迅猛发展，业界对实时计算的需求日益增高。Apache Flink作为一款低延迟、高吞吐、高性能兼备的实时流计算引擎，被广泛应用于广告推荐、风险预警、实时报表等场景中。

为了保证实时计算的正确性，Flink需要使用状态维护计算的中间状态。对于状态量庞大的计算场景，需要使用RocksDB等状态存储引擎完成状态的持久化存储。

在有状态用例中，RocksDB的点查、范围查询等状态操作通常需要和磁盘建立IO通信，导致状态存储操作成为主要性能瓶颈。例如，在nexmark大状态用例中，RocksDB的平均CPU占比高达45%。因此，提升Flink对RocksDB的使用效率，对提升Flink应用性能具有重要的价值和意义。

#### OmniStateStore介绍
OmniStateStore是介于Flink和RocksDB之间的中间层，北向对接Flink的状态访问接口层，南向对接RocksDB的jni接口，通过对Flink的轻量级修改加速Flink对RocksDB的使用效率。其主要创新在于：
1. 通过Flink语义状态缓存算法，同Key状态优先在内存中完成聚合，减少状态对RocksDB的访问频次；
2. 通过Flink智能多留感知算法，对于仅需要点读、点写的状态，将memTable数据结构替换为HashLinkList, 提升状态点读和点写效率；
3. 通过双流Join数据缓存算法，减少StreamJoinOperator的状态范围查询次数；
4. 通过动态Filter技术，过滤冗余状态查询操作；

#### 使能场景
1. OmniStateStore适用于Flink + RocksDB架构，通过提供插件jar包方式完成有状态用例性能加速；
2. OmniStateStore在Flink侧有轻量级代码修改, 若与用户修改内容存在冲突需要进行代码适配方可使能；
3. OmniStateStore的性能提升比依赖于用例的RocksDB占比和状态操作类型，对于RocksDB占比低的场景仅保证性能不劣化；

#### 版本说明
1. 适用于Flink1.16.3, 版本迁移需要进行轻量级代码适配;
2. 对RocksDB版本无依赖, Flink切换RocksDB版本后无需进行额外适配。

# 快速上手
#### 编译依赖
1. 软件依赖：
- JDK：1.8.0_432
- Maven: 3.6.3

#### 源码编译
1. 下载源代码：
从OpenEuler开源社区下载OmniStateStore的源代码到编译服务器上；
2. 执行编译命令：
```
sh scripts/build.sh
```
3. 检查编译成功的软件包。
编译成功则在目录target/下存在：
OmniStateStore软件包BoostKit-omniruntime-omniStateStore_1.x.x.zip。

# 安装部署
#### 软件安装
1. 登录待安装节点，上传软件包BoostKit-omniruntime-omniStateStore_1.x.x.zip到“${FLINK_HOME}/lib/”子目录下；
2. 解压软件包, 得到flink-alg-falcon.jar
```
unzip BoostKit-omniruntime-omniStateStore_1.x.x.zip
rm -rf BoostKit-omniruntime-omniStateStore_1.x.x.zip
```

#### 软件启动
1. 配置参数项：
根据业务使用情况和待安装部署的环境设置Flink的conf子目录下flink-conf.yaml中的相关配置项, 示例配置参数详见本工程conf/flink-conf.yaml；
2. 启动Flink任务，查看日志中的配置项，检查配置是否成功；
3. 执行Nexmark0.3 Q4用例，观察到Task Manager日志中打印“[FALCON]”关键词，说明OmniStateStore使能成功。

#### 软件卸载
1. 将“${FLINK_HOME}/lib/”目录下的flink-alg-falcon.jar删除；
2. 删除“${FLINK_HOME}/conf/flink-conf.yaml”中的相关配置参数。

# 贡献指南
如果使用过程中有任何问题，或者需要反馈特性需求和bug报告，可以提交issues联系我们。

# 免责声明
此代码仓参与openeuler软件开源，编码风格遵照原生开源软件，继承原生开源软件安全设计，不破坏原生开源软件设计及编码风格和方式，软件的任何漏洞与安全问题，均由相应的上游社区根据其漏洞和安全响应机制解决。请密切关注上游社区发布的通知和版本更新。鲲鹏计算社区对软件的漏洞及安全问题不承担任何责任。

# 许可证书
木兰宽松许可证（http://license.coscl.org.cn/MulanPSL2）

# 源码开放
https://atomgit.com/openeuler/OmniStateStore