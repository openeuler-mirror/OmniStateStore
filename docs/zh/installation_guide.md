# 1 安装指南
<font size=3>本文档提供OmniStateStore的安装说明，用户可以参阅该文档完成omniStateStore的安装和部署。</font>

---

## 1.1 环境要求
## 1.1.1 硬件要求
<font size=3>
在安装omniStateStore之前，请确保硬件环境已经满足安装部署的要求，硬件配置要求如表1所示。

**表1** 硬件配置要求
<table>
  <tbody>
    <tr>
      <td style="text-align: left;">处理器</td>
      <td style="text-align: left;">鲲鹏920系列处理器，鲲鹏950处理器</td>
    </tr>
    <tr>
      <td style="text-align: left;">内存</td>
      <td style="text-align: left;">256GB以上</td>
    </tr>
    <tr>
      <td style="text-align: left;">内存频率</td>
      <td style="text-align: left;">4800 MT/s</td>
    </tr>
    <tr>
      <td style="text-align: left;">网卡</td>
      <td style="text-align: left;">NA</td>
    </tr>
    <tr>
      <td style="text-align: left;">磁盘</td>
      <td style="text-align: left;">至少一块3.6TB或7.68TB的Nvme SSD</td>
    </tr>
  </tbody>
</table>
</font>

## 1.1.2 软件要求
<font size=3>
OmniStateStore软件安装前需要将前置依赖的软件安装成功，建议参考各软件安全标准规范安装，集群各节点的操作系统和软件要求如表2所示。

**表2** 软件配置要求
<table>
  <thead>
    <tr>
      <th style="text-align: left;">软件名称</th>
      <th style="text-align: left;">软件版本</th>
      <th style="text-align: left;">获取方式</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td style="text-align: left;">OS</td>
      <td style="text-align: left;">openEuler 22.03 LTS SP3</td>
      <td style="text-align: left;"><a href="https://www.openeuler.openatom.cn/zh/download/archive/detail/?version=openEuler%2022.03%20LTS%20SP3">link</a></td>
    </tr>
    <tr>
      <td style="text-align: left;">JDK</td>
      <td style="text-align: left;">JDK 1.8.0_432</td>
      <td style="text-align: left;"><a href="https://oraclelinux.pkgs.org/8/ol8-appstream-x86_64/java-1.8.0-openjdk-1.8.0.432.b06-2.0.1.el8.x86_64.rpm.html">link</a></td>
    </tr>
    <tr>
      <td style="text-align: left;">GCC</td>
      <td style="text-align: left;">10.3.1</td>
      <td style="text-align: left;"><a href="https://mirrors.huaweicloud.com/kunpeng/archive/compiler/kunpeng_gcc">link</a></td>
    </tr>
    <tr>
      <td style="text-align: left;">Flink</td>
      <td style="text-align: left;">1.16.3</td>
      <td style="text-align: left;"><a href="https://flink.apache.org/zh/downloads/">link</a></td>
    </tr>
  </tbody>
</table>
</font>

## 1.1.3 获取软件包
<font size=3>

**表2** OmniStateStore状态优化软件获取列表
<table>
  <thead>
    <tr>
      <th style="text-align: left;">名称</th>
      <th style="text-align: left;">包名</th>
      <th style="text-align: left;">发布类型</th>
      <th style="text-align: left;">说明</th>
      <th style="text-align: left;">获取地址</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td style="text-align: left;">omniStateStore状态优化压缩包</td>
      <td style="text-align: left;">BoostKit-omniruntime-omnistatestore-1.2.0.zip</td>
      <td style="text-align: left;">开源</td>
      <td style="text-align: left;">omniStateStore状态优化软件安装包</td>
      <td style="text-align: left;"><a href="https://atomgit.com/openeuler/OmniStateStore">link</a></td>
    </tr>
  </tbody>
</table>
</font>

---

## 1.2 安装omniStateStore
<font size=3>

**步骤一**&emsp;请参阅[OmniStateStore状态优化软件获取列表](#113-获取软件包)获取软件包BoostKit-omniruntime-omnistatestore-1.2.0.zip。

**步骤二**&emsp;配置环境变量，指定FLINK_HOME和JAVA_HOME，并配置LD_LIBRARY_PATH
```
LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$JAVA_HOME/lib:$JAVA_HOME/jre/lib/aarch64:$JAVA_HOME/jre/lib/aarch64/server:/usr/local/lib
```
**步骤三**&emsp;登录安装节点，将BoostKit-omniruntime-omnistatestore-1.2.0.zip解压到$FLINK_HOME/lib目录下，将librocksd.so.6拷贝到/usr/local/lib目录，将flink-alg-falcon.jar保留至当前目录中

```
unzip BoostKit-omniruntime-omnistatestore-1.2.0.zip
mv librocksdb.so.6 /usr/local/lib
rm -rf BoostKit-omniruntime-omnistatestore-1.2.0.zip
```
</font>

---

## 1.3 卸载omniStateStore
<font size=3>

**步骤一**&emsp;使用完omniStateStore后，进入$FLINK_HOME/lib目录卸载相关软件包。
```
rm -rf /usr/local/lib/librocksdb.so.6
rm -rf flink-alg-falcon.jar
```
</font>