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

OmniStateStore软件安装前需要将前置依赖的软件安装成功，建议参考各软件安全标准规范安装，集群各节点的操作系统和软件要求如表2所示。具体地，各软件的安装流程请参阅[软件安装教程](#14-其他软件安装指导)。

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
      <td style="text-align: left;">Maven</td>
      <td style="text-align: left;">Apache Maven 3.6.3</td>
      <td style="text-align: left;"><a href="https://link.csdn.net/?from_id=119428896&target=https%3A%2F%2Farchive.apache.org%2Fdist%2Fmaven%2Fmaven-3%2F3.6.3%2Fbinaries%2Fapache-maven-3.6.3-bin.zip
">link</a></td>
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
    <tr>
      <td style="text-align: left;">Docker</td>
      <td style="text-align: left;">18.09.0</td>
      <td style="text-align: left;">/</td>
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

**步骤二**&emsp;配置环境变量，指定FLINK_HOME和JAVA_HOME，并配置LD_LIBRARY_PATH。
```
LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$JAVA_HOME/lib:$JAVA_HOME/jre/lib/aarch64:$JAVA_HOME/jre/lib/aarch64/server:/usr/local/lib
```
**步骤三**&emsp;登录安装节点，将BoostKit-omniruntime-omnistatestore-1.2.0.zip解压到$FLINK_HOME/lib目录下，将librocksd.so.6拷贝到/usr/local/lib目录，将flink-alg-falcon.jar保留至当前目录中。

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

---

## 1.4 其他软件安装指导
## 1.4.1 JDK
<font size=3>

**步骤一**&emsp;下载JDK软件包，下载链接为[link](https://oraclelinux.pkgs.org/8/ol8-appstream-aarch64/java-1.8.0-openjdk-1.8.0.482.b08-1.0.1.el8.aarch64.rpm.html)。

**步骤二**&emsp;进入安装包存放目录，执行下列命令安装JDK软件包。
```
sudo yum localinstall java-1.8.0-openjdk-1.8.0.482.b08-1.0.1.el8.aarch64.rpm
ls -l /usr/java/ # jdk默认安装目录
```
**步骤三**&emsp;配置环境变量，在/etc/profile文件中添加：
```
export JAVA_HOME=/usr/java/jdk-1.8.0
export JRE_HOME=$JAVA_HOME/jre
export CLASSPATH=.:$JAVA_HOME/lib:$JRE_HOME/lib
export PATH=$JAVA_HOME/bin:$PATH
```
**步骤四**&emsp;更新环境变量。
```
source /etc/profile
java -version    
javac -version   # 查看jdk版本
```
</font>

## 1.4.2 Maven
<font size=3>

**步骤一**&emsp;下载Maven软件包，下载链接为[link](https://link.csdn.net/?from_id=119428896&target=https%3A%2F%2Farchive.apache.org%2Fdist%2Fmaven%2Fmaven-3%2F3.6.3%2Fbinaries%2Fapache-maven-3.6.3-bin.zip)。

**步骤二**&emsp;将软件包放置于安装目录中(以/opt目录为例)，完成软件包部署：
```
cd /opt
unzip apache-maven-3.6.3-bin.zip
rm -rf apache-maven-3.6.3-bin.zip
```
**步骤三**&emsp;配置环境变量，在/etc/profile文件中添加：
```
export MAVEN_HOME=/opt/apache-maven-3.6.3
export PATH=$MAVEN_HOME/bin:$PATH
```
**步骤四**&emsp;更新环境变量。
```
source /etc/profile
mvn -version # 查看maven版本
```
</font>

## 1.4.3 GCC
<font size=3>

**步骤一**&emsp;下载GCC二进制，下载链接为[link](https://mirrors.huaweicloud.com/kunpeng/archive/compiler/kunpeng_gcc/gcc-10.3.1-2021.09-aarch64-linux.tar.gz)。

**步骤二**&emsp;将软件包放置于安装目录中(以/opt目录为例)，完成软件包部署：
```
cd /opt
tar -zxvf gcc-10.3.1-2021.09-aarch64-linux.tar.gz
mv gcc-10.3.1-2021.09-aarch64-linux gcc-10.3.1
rm -rf gcc-10.3.1-2021.09-aarch64-linux.tar.gz
```
**步骤三**&emsp;配置环境变量，在/etc/profile文件中添加：
```
export GCC_HOME=/opt/gcc-10.3.1
export PATH=$GCC_HOME/bin:$PATH
export LD_LIBRARY_PATH=$GCC_HOME/lib64:$GCC_HOME/lib:$LD_LIBRARY_PATH
export CPLUS_INCLUDE_PATH=$GCC_HOME/include/c++/10.3.1:$GCC_HOME/include:$CPLUS_INCLUDE_PATH
```
**步骤四**&emsp;更新环境变量。
```
source /etc/profile
gcc --version
g++ --version  # 查看gcc版本
```
</font>

## 1.4.4 Flink
<font size=3>

**步骤一**&emsp;下载Flink软件包，下载链接为[link](https://archive.apache.org/dist/flink/flink-1.16.3)。

**步骤二**&emsp;将软件包放置于安装目录中(以/opt目录为例)，完成软件包部署：
```
cd /opt
tar -zxvf flink-1.16.3-bin-scala_2.12.tgz
mv flink-1.16.3-bin-scala_2.12 flink-1.16.3
rm -rf flink-1.16.3-bin-scala_2.12.tgz
```
**步骤三**&emsp;配置环境变量，在/etc/profile文件中添加：
```
export FLINK_HOME=/opt/flink-1.16.3
export PATH=FLINK_HOME/bin:$PATH
```
**步骤四**&emsp;更新环境变量。
```
source /etc/profile
```
</font>

## 1.4.5 Docker
<font size=3>

**步骤一**&emsp;使用以下命令安装docker：
```
sudo yum remove -y docker docker-client docker-client-latest docker-common docker-latest docker-latest-logrotate docker-logrotate docker-engine
sudo yum install -y yum-utils device-mapper-persistent-data lvm2
sudo yum-config-manager --add-repo https://mirrors.aliyun.com/docker-ce/linux/centos/docker-ce.repo
sudo yum install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo systemctl start docker
sudo systemctl enable docker
docker --version  # 检查docker版本
```

</font>