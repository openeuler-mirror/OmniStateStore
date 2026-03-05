# 1 快速入门
<font size=3> 本文档提供OmniStateStore的快速入门指南，用户可以参考本文档快速使能特性并验证OmniStateStore的加速能力。</font>

---

## 1.1 使用说明
<font size=3>
OmniStateStore的加速特性包含动态Filter技术、Flink语义状态缓存和Merge读写优化，具体地，可以拆分为以下子特性：<br>

- 通过**Flink语义状态缓存算法**，同Key状态优先在内存中完成聚合，减少状态对RocksDB的访问频次；<br>
- 通过**Flink智能多留感知算法**，对于仅需要点读、点写的状态，将memTable数据结构替换为HashLinkList, 提升状态点读和点写效率；<br>
- 通过**使用merge替换状态RMW**，减少Join算子的状态更新开销；<br>
- 通过**双流Join数据缓存算法**，减少StreamJoinOperator的状态范围查询次数；<br>
- 通过**动态Filter技术**，过滤冗余状态查询操作；<br>

OmniStateStore特性的使能过程中，存在以下约束：<br>
- **版本兼容性**：本项目适用于Flink1.16.3 + RocksDB6.20.3架构，仅在指定版本下保证数据一致性和特性加速效果。
- **侵入式修改**：OmniStateStore包含对Flink的轻量级修改，若用户也修改了Flink源码，需要处理冲突后方可使能特性。
- **加速效果**：OmniStateStore的加速效果与用例RocksDB占比成正比，且和用例状态类型相关。在RocksDB占比较低场景下仅保证性能无劣化，在不使用ValueState和MapState的用例上仅保证性能无劣化。

</font>

---

## 1.2 环境准备
<font size=3>

按照下表准备OmniStateStore的**编译环境**，以支持快速完成OmniStateStore安装。

**表1** OmniStateStore编译环境准备
<table>
  <thead>
    <tr>
      <th style="text-align: left;">准备内容</th>
      <th style="text-align: left;">说明</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td style="text-align: left;">服务器</td>
      <td style="text-align: left;">Kunpeng 920系列服务器 </td>
    </tr>
    <tr>
      <td style="text-align: left;">OS</td>
      <td style="text-align: left;">openEuler 22.03 LTS SP3</td>
    </tr>
    <tr>
      <td style="text-align: left;">JDK</td>
      <td style="text-align: left;">openJDK 1.8.0_432</td>
    </tr>
    <tr>
      <td style="text-align: left;">Maven</td>
      <td style="text-align: left;">Apache Maven 3.6.3</td>
    </tr>
    <tr>
      <td style="text-align: left;">GCC</td>
      <td style="text-align: left;">GCC 10.3.1</td>
    </tr>
  </tbody>
</table>

按照下表搭建OmniStateStore的**运行环境**，以支持快速验证OmniStateStore在有状态用例上的加速效果。

**表2** OmniStateStore运行环境准备
<table>
  <thead>
    <tr>
      <th style="text-align: left;">准备内容</th>
      <th style="text-align: left;">说明</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td style="text-align: left;">Flink</td>
      <td style="text-align: left;">Apache Flink 1.16.3</td>
    </tr>
    <tr>
      <td style="text-align: left;">RocksDB</td>
      <td style="text-align: left;">FRocksDB6.20.3</td>
    </tr>
    <tr>
      <td style="text-align: left;">Nexmark</td>
      <td style="text-align: left;">Nexmark0.3</td>
    </tr>
    <tr>
      <td style="text-align: left;">Docker</td>
      <td style="text-align: left;">Docker18.09.0</td>
    </tr>
  </tbody>
</table>
</font>

---

## 1.3 操作步骤
<font size=3>

**步骤一**&emsp;下载源码。从[gitCode](https://gitcode.com/openeuler/OmniStateStore.git)上下载omniStateStore的falcon分支源码。

**步骤二**&emsp;编译源码。使用[sh build.sh](../../scripts/build.sh)编译omniStateStore，在项目根目录生成BoostKit-omniruntime-omniStateStore-1.2.0.zip。omniStateStore的详细安装流程请参阅[installation_guide.md](./installation_guide.md)。

**步骤三**&emsp;环境部署。使用docker部署Flink容器化运行环境，创建一个JobManager容器和两个TaskManager容器，容器配置均为8C32GB。JobManager分配8GB内存，单个TaskManager分配2个TaskSlot和8GB内存。

**步骤四**&emsp;设置环境变量。在容器中部署指定版本的Flink和Nexmark，并配置JAVA_HOME，FLINK_HOME环境变量。此外，将LD_LIBRARY_PATH配置为
```
LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$JAVA_HOME/lib:$JAVA_HOME/jre/lib/aarch64:$JAVA_HOME/jre/lib/aarch64/server:/usr/local/lib
```

**步骤五**&emsp;安装omniStateStore。解压BoostKit-omniruntime-omniStateStore-1.2.0.zip，将librocksd.so.6拷贝到/usr/local/lib目录下，将flink-alg-falcon.jar拷贝到$FLINK_HOME/lib目录下。

**步骤六**&emsp;配置omniStateStore参数。参考[flink-conf.yaml](../../conf/flink-conf.yaml)，在$FLINK_HOME/conf/flink-conf.yaml中配置omniStateStore参数以使能加速特性，并指定使用rocksbd状态后端。

**步骤七**&emsp;加速效果验证。使用原生Flink运行nexmark q4用例，记录任务的单核吞吐。使能omniStateStore运行nexmark q4用例，观测到任务单核吞吐相比原生Flink有显著提升。用户可以在Flink的运行日志中搜索以下关键字，若观测到以下关键字，表示成功使能omniStateStore加速特性。<br>

```
[FALCON] enable miniBatch process for StreaminJoinOperator.
[FALCON] accState is valueState, use HashLinkList as memTable structure.
[FALCON] left-records is map, use range filter.
[FALCON] <accState, VALUE> enable falcon cache.
[FALCON] merge operation is used for left-records.
```
</font>