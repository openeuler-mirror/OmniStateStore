# 安装和使用指南<a name="ZH-CN_TOPIC_0000002550026769"></a>

## 环境要求<a name="ZH-CN_TOPIC_0000002518426734"></a>

安装和使用OmniStateStore之前，请确保软硬件环境已经满足安装部署及应用程序正常运行的要求。

**硬件要求<a name="zh-cn_topic_0000002466592290_section10915213184318"></a>**

OmniStateStore软件在Docker容器中运行，硬件要求如[**表 1** 硬件配置要求](#硬件配置要求)所示。

**表 1** 硬件配置要求<a id="硬件配置要求"></a>

<table>
<tbody><tr><th><p>处理器</p>
</th>
<td><p>鲲鹏920系列处理器</p>
</td>
</tr>
<tr><th><p>内存大小</p>
</th>
<td><p>256GB以上</p>
</td>
</tr>
<tr><th><p>内存频率</p>
</th>
<td><p>4800MT/s</p>
</td>
</tr><tr><th><p>网卡</p>
</th>
<td><p>NA</p>
</td>
</tr>
<tr><th><p>磁盘（NVMe SSD）</p>
</th>
<td><p>至少一块3.6TB或7.68TB磁盘</p>
</td>
</tbody>
</table>

**软件要求<a name="zh-cn_topic_0000002466592290_section5999819104313"></a>**

OmniStateStore软件安装前需要将前置依赖的软件安装成功，建议参考各软件安全标准规范安装，集群中各节点的操作系统和软件要求如[**表 2** 软件要求](#软件要求)所示。

**表 2** 软件要求<a id="软件要求"></a>

|软件名称|软件版本|获取方式|
|--|--|--|
|OS|openEuler 22.03 LTS SP3|[获取链接](https://easysoftware.openeuler.openatom.cn/zh/field?os=openEuler-22.03-LTS-SP3)|
|Java|JDK 1.8.0_432|[获取链接](https://www.oracle.com/apac/java/technologies/downloads/#java8)|
|Flink|<ul><li>1.16.1<li>1.16.3<li>1.17.1|[获取链接](https://archive.apache.org/dist/flink/)|


**获取软件包<a name="zh-cn_topic_0000002466592290_zh-cn_topic_0000001664980070_section3489574613"></a>**

**表 3** OmniStateStore状态优化软件获取列表<a id="OmniStateStore状态优化软件获取列表"></a>

||名称|包名|发布类型|说明|获取地址|
|--|--|--|--|--|
|OmniStateStore状态优化压缩包|BoostKit-omniruntime-omnistatestore-1.1.0.zip|开源|OmniStateStore状态优化软件安装包。|[获取链接](https://atomgit.com/openeuler/OmniStateStore/releases)|


**验证软件包完整性<a name="zh-cn_topic_0000002466592290_section15465143812508"></a>**

为了防止软件包在传递过程或存储期间被恶意篡改，获取软件包时需下载对应的数字签名文件用于完整性验证。

1. 参见[**表 3** OmniStateStore状态优化软件获取列表](#OmniStateStore状态优化软件获取列表)获取软件包。
2. 获取《OpenPGP签名验证指南》。
    - 运营商客户：请访问[http://support.huawei.com/carrier/digitalSignatureAction](http://support.huawei.com/carrier/digitalSignatureAction)
    - 企业客户：请访问[https://support.huawei.com/enterprise/zh/tool/pgp-verify-TL1000000054](https://support.huawei.com/enterprise/zh/tool/pgp-verify-TL1000000054)

3. 根据《OpenPGP签名验证指南》进行软件包完整性检查。

    >![](public_sys-resources/icon-note.gif) **说明：** 
    >-   如果校验失败，请不要使用该软件包，先联系华为技术支持工程师解决。
    >-   使用软件包安装或升级之前，也需要按上述过程先验证软件包的数字签名，确保软件包未被篡改。

安装和使用OmniStateStore之前，请确保软硬件环境已经满足安装部署及应用程序正常运行的要求。
## 安装OmniStateStore<a name="ZH-CN_TOPIC_0000002518426732"></a>

1. 请参见[**表 3** OmniStateStore状态优化软件获取列表](#OmniStateStore状态优化软件获取列表)获取软件包BoostKit-omniruntime-omnistatestore-1.1.0.zip。
2. 登录安装节点，并上传软件包BoostKit-omniruntime-omnistatestore-1.1.0.zip至“$\{FLINK\_HOME\}/lib/“子目录下。
3. 解压软件包。

    ```
    unzip BoostKit-omniruntime-omnistatestore-1.1.0.zip
    tar -zxvf BoostKit-omniruntime-omnistatestore-1.1.0.tar.gz
    ```

4. 将Flink版本对应的JAR包复制到“$\{FLINK\_HOME\}/lib/“目录。

    以1.16.3版本为例。

    ```
    cp BoostKit-omnistatestore_1.1.0/java/jars/flink-boost-statebackend-1.1.0-SNAPSHOT-for-flink-1.16.3.jar ./
    ```

5. 为了不占用磁盘空间，请执行以下命令删除软件包。

    ```
    rm -f BoostKit-omniruntime-omnistatestore-1.1.0.tar.gz
    rm -f BoostKit-omniruntime-omnistatestore-1.1.0.zip
    rm -rf BoostKit-omnistatestore_1.1.0/
    ```


## 启动OmniStateStore<a name="ZH-CN_TOPIC_0000002550026579"></a>

本章节介绍如何启动OmniStateStore服务，以启用Flink状态存储的加速功能。

1. 根据业务使用情况和待安装部署的环境，设置Flink的conf子目录下flink-conf.yaml中的相关配置项。

    配置项格式为_$\{配置项名称\} + $\{英文冒号\} + $\{空格\} + $\{配置项值\}_。OmniStateStore相关配置项说明请参见[配置项说明](配置项说明.md#ZH-CN_TOPIC_0000002550026577)。以下为不同场景的配置项示例说明。

    - 使能OmniStateStore特性必须要在“$\{FLINK\_HOME\}/conf/flink-conf.yaml“中新增或修改的配置项，需要在Job Manager和所有Task Manager的配置文件中同步进行修改。

        **表 1** 同步配置项说明<a id="同步配置项说明"></a>

|配置项名称|简要描述|配置示例|注意事项|
|--|--|--|--|
|state.backend|Flink开源参数，用于配置state.backend状态后端。|com.huawei.ock.bss.OckDBStateBackendFactory|此配置用于切换状态后端的类型，需要保证字符完全正确并区分大小写。|
|state.backend.ockdb.localdir|OmniStateStore状态数据本地存储路径。|/usr/local/flink/ockdb|保证路径已存在且对Flink运行用户有读写权限。|
|state.backend.ockdb.jni.logfile|OmniStateStore日志路径。|/usr/local/flink/log/kv.log|建议配置为Flink的日志目录。|


        配置项示例如下所示。

        ```
        state.backend: com.huawei.ock.bss.OckDBStateBackendFactory
        state.backend.ockdb.localdir: /usr/local/flink/ockdb
        state.backend.ockdb.jni.logfile: /usr/local/flink/log/kv.log
        ```

    - 开启Priority Queue持久化存储

        配置项示例如下所示。

        ```
        state.backend.ockdb.timer-service.factory: OCKDB
        ```

    - 开启KV分离存储

        配置项示例如下所示。

        ```
        state.backend.ockdb.kv-separate.switch: true
        state.backend.ockdb.kv-separate.threshold: 200
        ```

2. 执行以下命令，创建必要的目录。

    示例中，state.backend.ockdb.localdir配置为“/usr/local/flink/ockdb“，state.backend.ockdb.checkpoint.backup配置为“/usr/local/flink/checkpoint/backup“，安装时请以实际配置为准。

    ```
    mkdir -p /usr/local/flink/ockdb
    mkdir -p /usr/local/flink/checkpoint/backup
    ```

3. 启动Flink任务，查看日志中的配置项，检查配置是否成功。
4. 执行“$\{FLINK\_HOME\}/examples/streaming/WordCount.jar“示例应用程序。

    观察到Task Manager日志中打印“OmniStateStore service start success.”，说明OmniStateStore启动成功。

## 维护特性<a name="ZH-CN_TOPIC_0000002549906573"></a>

升级或卸载OmniStateStore时请满足操作规范。

**升级软件<a name="zh-cn_topic_0000002516475165_section8320132214814"></a>**

请参见[安装OmniStateStore](安装OmniStateStore.md#ZH-CN_TOPIC_0000002518426732)，在安装节点使用新版本的JAR包替换原有JAR包，无需卸载旧版本。

**卸载软件<a name="zh-cn_topic_0000002516475165_section777110331398"></a>**

>![](public_sys-resources/icon-notice.gif) **须知：** 
>当前步骤仅供需要卸载OmniStateStore时参考，不属于部署OmniStateStore的必要操作步骤。

1. 在安装节点将配置的state.backend.ockdb.localdir路径删除。
2. 将“\$\{FLINK\_HOME\}/lib/“目录下的flink-boost-statebackend-1.1.0-SNAPSHOT-for-flink-$\{flink._x.x.x_\}.jar删除。

    ```
    rm -f flink-boost-statebackend-1.1.0-SNAPSHOT-for-flink-x.x.x.jar
    ```

3. 将flink-conf.yaml配置文件中的state.backend切换为其他状态后端。

升级或卸载OmniStateStore时请满足操作规范。
## 相关参考<a name="ZH-CN_TOPIC_0000002518266822"></a>

### 配置项说明<a name="ZH-CN_TOPIC_0000002550026577"></a>

OmniStateStore的Log模块、StateStore模块和Metric模块的配置参数规范，涵盖日志管理、状态存储、性能监控等维度，为OmniStateStore在Flink场景下的部署与调优提供参数配置参考。

Log模块、StateStore模块和Metric模块的具体配置项说明请参见[**表 1** Log模块配置项说明](#Log模块配置项说明)、[**表 2** StateStore配置项说明](#StateStore配置项说明)和[**表 3** Metric配置项说明](#Metric配置项说明)。

**表 1** Log模块配置项说明<a id="Log模块配置项说明"></a>

|配置项名称|说明|默认值|合法值/区间|注意事项|
|--|--|--|--|--|
|state.backend.ockdb.jni.logfile|日志路径及日志文件名。|/usr/local/flink/log/kv.log|Flink运行用户具有读写权限的路径下面的文件（要求路径已存在）|保证路径已存在且对Flink运行用户有读写权限。|
|state.backend.ockdb.jni.loglevel|日志级别。1：DEBUG2：INFO3：WARN4：ERROR|2|[1, 4]|无特别说明|
|state.backend.ockdb.jni.lognum|最大日志文件个数。|20|[10, 50]|无特别说明|
|state.backend.ockdb.jni.logsize|单个日志文件大小。单位MB。|20|[10, 50]|无特别说明|


**表 2** StateStore配置项说明<a id="StateStore配置项说明"></a>

|配置项名称|说明|默认值|合法值/区间|注意事项|
|--|--|--|--|--|
|state.backend|Flink开源参数，用于配置state.backend状态后端。|无|com.huawei.ock.bss.OckDBStateBackendFactory|保证字符完全正确，区分大小写。|
|state.backend.ockdb.localdir|OmniStateStore本地数据存储路径。|无|已存在且Flink运行用户具有读写权限的路径。|保证路径已存在且对Flink运行用户有读写权限。确保该路径与taskmanager.state.local.root-dirs配置路径在同一个文件系统下。|
|taskmanager.state.local.root-dirs|Flink开源参数，用于配置本地Checkpoint临时目录。|无|已存在且Flink运行用户具有读写权限的路径。|建议配置。如果不配置，默认使用io.tmp.dirs配置的路径。确保该路径与state.backend.ockdb.localdir配置路径在同一个文件系统下。|
|state.backend.ockdb.savepoint.sort.local.dir|使用savepoint功能时需要配置，创建savepoint过程产生的用于排序的临时文件存放路径。|/usr/local/flink/savepoint/tmp|已存在且Flink运行用户具有读写权限的路径。|保证路径已存在且对Flink运行用户有读写权限。|
|state.backend.ockdb.jni.slice.watermark.ratio|缓存层通过设定高/低水位线比例阈值触发数据淘汰机制，将冷数据按预设策略迁移至LSM文件存储层，实现存储资源动态平衡。|0.8|(0, 1)|一般情况下不需要单独设置。|
|state.backend.ockdb.file.memory.fraction|控制用于读写LSM层数据的内存缓存空间大小占整个DB实例的内存上限的比例。|0.2|[0.1, 0.5]|一般情况下不需要单独设置。|
|state.backend.ockdb.jni.lsmstore.compaction.switch|LSM文件存储层整理合并开关。LSM文件存储层的分层合并机制通过开关控制数据文件的整理与合并操作，以优化存储性能和空间利用率。|1|0：关闭1：开启|建议开启。|
|state.backend.ockdb.ttl.filter.switch|TTL过期数据后台压缩清理。|false|false：关闭true：开启|当存在使用TTL State的业务场景时，建议开启。|
|state.backend.ockdb.lsmstore.compression.policy|LsmStore中的各层级Level的压缩策略。state.backend.ockdb.lsmstore.compression.level.policy默认值配合使用。level0：不开启压缩level1：不开启压缩level2：开启lz4压缩其余level：全压缩|lz4|none：不压缩lz4：使用lz4压缩|当Checkpoint文件上传过大时，建议开启。|
|state.backend.ockdb.lsmstore.compression.level.policy|手动配置LSM文件不同level配置压缩策略，默认值为“none,none,lz4”，表示level0不开启压缩，level1不开启压缩，level2开启lz4压缩。|none,none,lz4|none：不压缩lz4：使用lz4压缩|当Checkpoint成为瓶颈时，可适当将压缩策略往低层级提前，默认level层级范围[0, 5]。level0为前台写压缩，建议使用None。其余level为后台压缩。|
|state.backend.ockdb.lazy.download.switch|从Checkpoint恢复时启动懒加载开关。|false|false：关闭true：开启|当Checkpoint很大时开启，缩短任务恢复为running的所需时间。|
|state.backend.ockdb.bloom.filter.switch|针对状态Key的布隆过滤器开关。|true|false：关闭true：开启|对于存在较多无效key访问的场景建议开启。开启时会增加数十兆字节的内存占用。|
|state.backend.bloom.filter.expected.key.count|单个状态中布隆过滤器需要过滤的key的数量级。|8000000|[1000000, 10000000]|一般情况下不需要单独设置，配置数量越大，布隆过滤器需要占用的内存越多。|
|state.backend.ockdb.cache.filter.and.index.switch|开启LSM层filter与indexBlock使用LRU缓存的开关。|true|false：关闭true：开启|一般情况下不需要单独设置，文件数量大时，频繁读不同文件时建议开启。|
|state.backend.ockdb.cache.filter.and.index.ratio|filter与indexBlock独占缓存占总缓存的内存比，此内存不参与LRU压力过载释放。|0|(0, 1)|一般情况下不需要单独设置。压力过大，filter与indexBlock频繁在缓存中被释放时建议开启。|
|state.backend.ockdb.checkpoint.backup|开启本地恢复时Checkpoint本地备份slice文件的目录。|无|Flink运行用户具有读写权限的路径下面的文件（要求路径已存在）。|在开启本地恢复时需要配置，保证路径已存在且对Flink运行用户有读写权限。|
|state.backend.ockdb.timer-service.factory|控制Flink计时器存储的位置。|OCKDB|OCKDB：持久化存储在状态后端HEAP：存储在JVM堆内存中|当计时器数量较少时，基于堆的计时器可以具有更好的性能。|
|state.backend.ockdb.kv-separate.switch|控制KV分离启用的开关。|false|false：关闭true：开启|Value值比较大时开启KV分离。|
|state.backend.ockdb.kv-separate.threshold|KV分离启用的阈值，大于该值需KV分离。|200|(8, 4294967295)|大于该值的Value会经过KV分离单独存储。|


**表 3** Metric配置项说明<a id="Metric配置项说明"></a>

|配置项名称|说明|默认值|合法值/区间|注意事项|
|--|--|--|--|--|
|state.backend.ockdb.metric.enable|Metric功能总开关，开启后OmniStateStore才会采集Metric信息。|false|false：关闭true：开启|此开关打开后，各模块的Metric开关才会生效。|
|state.backend.ockdb.metric.memory|MemoryManager模块Metric信息采集开关。|false|false：关闭true：开启|无特别说明|
|state.backend.ockdb.metric.fresh.table|FreshTable模块Metric信息采集开关。|false|false：关闭true：开启|无特别说明|
|state.backend.ockdb.metric.slice.table|SliceTable模块Metric信息采集开关。|false|false：关闭true：开启|无特别说明|
|state.backend.ockdb.metric.lsm.store|LSM Store模块Metric信息采集开关。|false|false：关闭true：开启|无特别说明|
|state.backend.ockdb.metric.lsm.cache|LSM Cache模块Metric信息采集开关。|false|false：关闭true：开启|无特别说明|
|state.backend.ockdb.metric.snapshot|Snapshot模块Metric信息采集开关。|false|false：关闭true：开启|无特别说明|
|state.backend.ockdb.metric.restore|Restore模块Metric信息采集开关。|false|false：关闭true：开启|无特别说明|


OmniStateStore的Log模块、StateStore模块和Metric模块的配置参数规范，涵盖日志管理、状态存储、性能监控等维度，为OmniStateStore在Flink场景下的部署与调优提供参数配置参考。
### Metric指标<a name="ZH-CN_TOPIC_0000002550026581"></a>

OmniStateStore支持对接Flink Metric框架，并提供一系列Metric指标，用于在任务运行过程中监测OmniStateStore的内存占用、缓存命中率等内部运行状态信息，为OmniStateStore在Flink场景下的性能调优与运行状态分析提供了Metric指标参考。

用户可以通过Flink WebUI上任务运行时的Metric界面添加并查看这些指标，便于实时了解和分析OmniStateStore的运行表现。

>![](public_sys-resources/icon-notice.gif) **须知：** 
>-   采集Metric数据会产生额外的性能开销，可能对任务的运行性能造成影响，建议仅在任务测试阶段或对性能要求不高的任务中开启Metric功能。
>-   所有数据量相关指标的单位为字节，所有耗时相关指标的单位为秒。

**MemoryManager模块<a name="zh-cn_topic_0000002505799817_section7735163610488"></a>**

**表 1** Metric指标参考<a id="Metric指标参考"></a>

|Metric指标项|说明|
|--|--|
|ockdb_memory_used_fresh|FreshTable类型的内存使用量|
|ockdb_memory_used_slice|SliceTable类型的内存使用量|
|ockdb_memory_used_file|LSMStore类型的内存使用量|
|ockdb_memory_used_snapshot|Snapshot类型的内存使用量|
|ockdb_memory_used_borrow_heap|BorrowHeap类型的内存使用量|
|ockdb_memory_used_db|单个TaskSlot的托管内存使用总量|
|ockdb_memory_max_fresh|FreshTable类型的内存分配总量|
|ockdb_memory_max_slice|SliceTable类型的内存分配总量|
|ockdb_memory_max_file|LSMStore类型的内存分配总量|
|ockdb_memory_max_snapshot|Snapshot类型的内存分配总量|
|ockdb_memory_max_borrow_heap|BorrowHeap类型的内存分配总量|
|ockdb_memory_max_db|单个TaskSlot的托管内存分配总量|


**FreshTable模块<a name="zh-cn_topic_0000002505799817_section12799165145018"></a>**

**表 2** Metric指标参考<a id="Metric指标参考_1"></a>

|Metric指标项|说明|
|--|--|
|ockdb_fresh_hit_count|FreshTable访问命中次数|
|ockdb_fresh_miss_count|FreshTable访问未命中次数|
|ockdb_fresh_record_count|FreshTable访问总记录次数|
|ockdb_fresh_flushing_record_count|FreshTable正在淘汰的KV记录数|
|ockdb_fresh_flushing_segment_count|FreshTable正在淘汰的Segment数量|
|ockdb_fresh_flushed_record_count|FreshTable已淘汰的KV记录数|
|ockdb_fresh_flushed_segment_count|FreshTable已淘汰的Segment数量|
|ockdb_fresh_segment_create_fail_count|FreshTable创建Segment时内存不足导致失败的次数|
|ockdb_fresh_flush_count|FreshTable淘汰数据到SliceTable的总次数|
|ockdb_fresh_binary_key_size|FreshTable中当前所有Key的总大小|
|ockdb_fresh_binary_value_size|FreshTable中当前所有Value的总大小|
|ockdb_fresh_binary_map_node_size|FreshTable中当前所有MapNode的总大小|
|ockdb_fresh_wasted_size|FreshTable的Segment淘汰到SliceTable时，Segment空闲空间累积总大小|


**SliceTable模块<a name="zh-cn_topic_0000002505799817_section9213174155116"></a>**

**表 3** Metric指标参考<a id="Metric指标参考_2"></a>

|Metric指标项|说明|
|--|--|
|ockdb_slice_hit_count|SliceTable访问命中次数|
|ockdb_slice_miss_count|SliceTable访问未命中次数|
|ockdb_slice_read_count|SliceTable访问总记录次数|
|ockdb_slice_read_avg_size|请求访问时平均遍历SliceTable中Slice链的长度|
|ockdb_slice_evict_waiting_count|待淘汰的Slice数量|
|ockdb_slice_compaction_count|SliceTable已完成的Compaction任务数|
|ockdb_slice_compaction_slice_count|SliceTable完成Compaction的Slice总数量|
|ockdb_slice_compaction_avg_slice_count|SliceTable每次Compaction任务平均处理的Slice数量|
|ockdb_slice_chain_avg_size|SliceChain的平均长度|
|ockdb_slice_avg_size|单个Slice的平均大小|


**FileCache模块<a name="zh-cn_topic_0000002505799817_section1596412433523"></a>**

**表 4** Metric指标参考<a id="Metric指标参考_3"></a>

|Metric指标项|说明|
|--|--|
|ockdb_index_block_hit_count|IndexBlock在BlockCache中的访问命中次数|
|ockdb_index_block_hit_size|IndexBlock在BlockCache访问命中的数据量大小|
|ockdb_index_block_miss_count|IndexBlock在BlockCache访问未命中次数|
|ockdb_index_block_miss_size|IndexBlock在BlockCache访问未命中的数据量大小|
|ockdb_index_block_cache_count|IndexBlock在BlockCache中的缓存个数|
|ockdb_index_block_cache_size|IndexBlock在BlockCache中的缓存大小|
|ockdb_data_block_hit_count|DataBlock在BlockCache访问命中次数|
|ockdb_data_block_hit_size|DataBlock在BlockCache访问命中数据量大小|
|ockdb_data_block_miss_count|DataBlock在BlockCache访问未命中次数|
|ockdb_data_block_miss_size|DataBlock在BlockCache访问未命中数据量大小|
|ockdb_data_block_cache_count|DataBlock在BlockCache中的缓存个数|
|ockdb_data_block_cache_size|DataBlock在BlockCache中的缓存大小|
|ockdb_filter_hit_count|FilterBlock在BlockCache访问命中次数|
|ockdb_filter_hit_size|FilterBlock在BlockCache访问命中的数据量大小|
|ockdb_filter_miss_count|FilterBlock在BlockCache访问未命中次数|
|ockdb_filter_miss_size|FilterBlock在BlockCache访问未命中的数据量大小|
|ockdb_filter_cache_count|FilterBlock在BlockCache中的缓存个数|
|ockdb_filter_cache_size|FilterBlock在BlockCache中的缓存大小|
|ockdb_filter_success_count|FilterBlock对Key过滤结果为不存在的次数|
|ockdb_filter_exist_success_count|FilterBlock对Key过滤结果为存在且实际存在的次数|
|ockdb_filter_exist_fail_count|FilterBlock对Key过滤结果为存在且实际不存在的次数|


**FileStore模块<a name="zh-cn_topic_0000002505799817_section1634518277549"></a>**

**表 5** Metric指标参考<a id="Metric指标参考_4"></a>

|Metric指标项|说明|
|--|--|
|ockdb_lsm_flush_count|LSMStore模块Flush到磁盘的文件总数|
|ockdb_lsm_flush_size|LSMStore模块Flush到磁盘的文件数据量总大小|
|ockdb_lsm_compaction_count|LSMStore完成的Compaction任务总次数|
|ockdb_lsm_hit_count|LSMStore访问命中次数|
|ockdb_lsm_miss_count|LSMStore访问未命中次数|
|ockdb_level0_hit_count|LSMStore的Level0层文件访问命中次数|
|ockdb_level0_miss_count|LSMStore的Level0层文件访问未命中次数|
|ockdb_level1_hit_count|LSMStore的Level1层文件访问命中次数|
|ockdb_level1_miss_count|LSMStore的Level1层文件访问未命中次数|
|ockdb_level2_hit_count|LSMStore的Level2层文件访问命中次数|
|ockdb_level2_miss_count|LSMStore的Level2层文件访问未命中次数|
|ockdb_above_level2_hit_count|LSMStore的Level3及以上层文件访问命中次数|
|ockdb_above_level2_miss_count|LSMStore的Level3及以上层文件访问未命中次数|
|ockdb_level0_file_size|LSMStore的Level0层文件数据量总大小|
|ockdb_level1_file_size|LSMStore的Level1层文件数据量总大小|
|ockdb_level2_file_size|LSMStore的Level2层文件数据量总大小|
|ockdb_level3_file_size|LSMStore的Level3层文件数据量总大小|
|ockdb_above_level3_file_size|LSMStore的Level4及以上层文件数据量总大小|
|ockdb_lsm_file_size|LSMStore所有层文件数据量总大小|
|ockdb_lsm_compaction_read_size|LSMStore Compaction任务执行时读取文件的总大小|
|ockdb_lsm_compaction_write_size|LSMStore Compaction任务执行时写入文件的总大小|
|ockdb_level0_compaction_rate|LSMStore的Level0层文件压缩率|
|ockdb_level1_compaction_rate|LSMStore的Level1层文件压缩率|
|ockdb_level2_compaction_rate|LSMStore的Level2层文件压缩率|
|ockdb_level3_compaction_rate|LSMStore的Level3层文件压缩率|
|ockdb_lsm_compaction_rate|LSMStore所有层文件总压缩率|
|ockdb_lsm_file_count|LSMStore所有层文件总数量|


**Snapshot模块<a name="zh-cn_topic_0000002505799817_section15501109155814"></a>**

**表 6** Metric指标参考<a id="Metric指标参考_5"></a>

|Metric指标项|说明|
|--|--|
|ockdb_snapshot_total_time|最近一次快照任务执行总耗时|
|ockdb_snapshot_upload_time|最近一次快照任务上传数据耗时|
|ockdb_snapshot_file_count|最近一次快照任务创建的文件数量|
|ockdb_snapshot_file_size|最近一次快照任务创建的文件大小|
|ockdb_snapshot_incremental_size|最近一次快照任务创建的增量文件大小|
|ockdb_snapshot_slice_file_count|最近一次快照任务创建的SliceTable快照文件数量|
|ockdb_snapshot_slice_incremental_file_size|最近一次快照任务创建的SliceTable增量文件大小|
|ockdb_snapshot_slice_file_size|最近一次快照任务创建的SliceTable快照文件大小|
|ockdb_snapshot_sst_file_count|最近一次快照任务创建的LSMStore快照文件数量|
|ockdb_snapshot_sst_incremental_file_size|最近一次快照任务创建的LSMStore增量文件大小|
|ockdb_snapshot_sst_file_size|最近一次快照任务创建的LSMStore快照文件大小|


**Restore模块<a name="zh-cn_topic_0000002505799817_section19402193125815"></a>**

**表 7** Metric指标参考<a id="Metric指标参考_6"></a>

|Metric指标项|说明|
|--|--|
|ockdb_restore_total_time|最近一次快照恢复任务总耗时|
|ockdb_restore_download_time|最近一次快照恢复任务下载耗时|
|ockdb_restore_lazy_download_time|最近一次快照恢复任务懒加载耗时|


OmniStateStore支持对接Flink Metric框架，并提供一系列Metric指标，用于在任务运行过程中监测OmniStateStore的内存占用、缓存命中率等内部运行状态信息，为OmniStateStore在Flink场景下的性能调优与运行状态分析提供了Metric指标参考。
### 功能规格<a name="ZH-CN_TOPIC_0000002518266820"></a>

对比OmniStateStore与RocksDB作为Flink状态后端在基本状态读写、Checkpoint、Savepoint等核心功能上的支持情况，两者在功能点上均保持一致支持，为用户评估OmniStateStore替代RocksDB的可行性提供参考依据。

开源Flink使用的RocksDB状态后端功能与OmniStateStore功能对比详见[**表 1** 状态后端功能对比](#状态后端功能对比)。

**表 1** 状态后端功能对比<a id="状态后端功能对比"></a>

|功能分类|功能点|RocksDB StateBackend|OmniStateStore StateBackend|
|--|--|--|--|
|基本状态读写API|Operator State|支持|支持|
|基本状态读写API|Broadcast State|支持|支持|
|基本状态读写API|Value State|支持|支持|
|基本状态读写API|List State|支持|支持|
|基本状态读写API|Map State|支持|支持|
|基本状态读写API|Reducing State|支持|支持|
|基本状态读写API|Aggregating State|支持|支持|
|基本状态读写API|状态有效期（TTL）|支持|支持|
|基本状态读写API|计时器（Timer）|支持|支持|
|Checkpoint|全量快照|支持|支持|
|Checkpoint|增量快照|支持|支持|
|Checkpoint|对齐快照|支持|支持|
|Checkpoint|非对齐快照|支持|支持|
|Checkpoint|普通快照恢复|支持|支持|
|Checkpoint|扩缩并行度场景下快照恢复|支持|支持|
|Savepoint|不停作业执行Savepoint|支持|支持|
|Savepoint|停作业执行Savepoint|支持|支持|
|Savepoint|标准格式Savepoint|支持|支持|
|Savepoint|原生格式Savepoint|支持|支持|
|Savepoint|删除Savepoint|支持|支持|
|Savepoint|普通Savepoint恢复|支持|支持|
|Savepoint|扩缩并行度场景下Savepoint恢复|支持|支持|
|Savepoint|Savepoint支持状态数据结构升级|支持|支持|


对比OmniStateStore与RocksDB作为Flink状态后端在基本状态读写、Checkpoint、Savepoint等核心功能上的支持情况，两者在功能点上均保持一致支持，为用户评估OmniStateStore替代RocksDB的可行性提供参考依据。


