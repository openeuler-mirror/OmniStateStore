# 1 常见问题
<font size=3> 本文档记录OmniStateStore安装和使能过程中的常见问题及其解决方案。</font>

---

## 1.1 omniStateStore和flink版本不适配导致任务无法正常启动
<font size=3>

**问题现象<br>**
使用Flink1.16.1运行nexmark0.3-Q4用例，并尝试使能omniStateStore加速特性，出现类构造器未查询到的问题。如下图所示：<br>

**图1** OmniStateStore运行报错截图

<a href="./figures/omniStateStore faq1.png"><img src="./figures/omniStateStore faq1.png" alt="faq" width="1200" /></a>

**问题原因<br>**
目前，omniStateStore只支持Flink1.16.3 + FRocksDB6.20.3。由于omniStateStore需要对Flink进行轻量级修改，若用户也修改了Flink，可能和omniStateStore产生冲突。<br>
omniStateStore对RocksDBResourceContainer所在的类进行了轻量级代码修改，而Flink1.16.1的同名类也在Flink1.16.3的基础上进行了代码修改，两部分修改存在冲突。而Flink任务优先加载了omniStateStore的同名class，导致RocksDBResourceContainer的构造器未找到。

**解决方案<br>**
回退到Flink1.16.3版本进行测试，或是将Flink1.16.1的修改内容合入omniStateStore中重新编译出包测试。

</font>

---

## 1.2 omniStateStore未正确部署导致任务无法正常启动
<font size=3>

**问题现象<br>**
Flink任务运行时未成功加载librocksdb.so.6，导致任务运行失败。如下图所示：<br>

**图2** OmniStateStore JobManager运行报错截图

<a href="./figures/omniStateStore faq2-1.png"><img src="./figures/omniStateStore faq2-1.png" alt="faq" width="1200" /></a>


**图3** OmniStateStore TaskManager运行报错截图

<a href="./figures/omniStateStore faq2-2.png"><img src="./figures/omniStateStore faq2-2.png" alt="faq" width="1200" /></a>

**问题原因<br>**
omniStateStore运行时依赖librocksdb.so.6，而Flink任务会从LD_LIBRARY_PATH目录中加载该动态库。若未按照要求将动态库部署到指定路径中，或是未正确配置LD_LIBRARY_PATH环境变量，将导致任务运行失败。

**解决方案<br>**
检查LD_LIBRARY_PATH环境变量，并将librocksdb.so.6拷贝到指定目录下。omniStateStore的安装流程请参阅[安装指南](installation_guide.md)。

</font>