# OmniStateStore编译与测试环境镜像

本目录提供预装工具链和UT依赖的环境镜像。镜像不包含项目源码、编译产物或Flink，启动后挂载源码并执行项目构建脚本。完整流程见[容器环境部署](../docs/zh/installation_guide.md#容器环境部署)。

## 镜像内容

| 项目 | 内容 |
| --- | --- |
| 基础系统 | openEuler 24.03 LTS-SP3 |
| 工具链 | GCC/G++、CMake、Make、Git |
| Java | OpenJDK 8开发包、Maven；设置JAVA_HOME |
| UT依赖 | libaio-devel、libasan |
| Maven缓存 | 尝试预取Flink 1.16.1、1.16.3、1.17.1、1.20.0的依赖和插件 |

依赖预热允许失败，不保证离线编译；首次构建仍可能访问Maven仓库。预热使用上游master，可通过`MAVEN_REPO_REF`指定分支或标签。缓存不会覆盖运行时挂载的源码。Dockerfile中的自检只检查工具链、JNI头文件及ASan/libaio链接运行，不代表项目UT通过。

## 构建

在Linux主机的仓库根目录执行：

```bash
docker build -f docker/Dockerfile \
    -t omnistatestore-verify:24.03-lts-sp3-$(uname -m) .
```

`--build-arg PREWARM_MAVEN=0`关闭依赖预热。`--build-arg BASE_IMAGE=hub.oepkgs.net/openeuler/openeuler:22.03-lts-sp3`切换到22.03基线，切换后需重新验证并使用相应镜像标签。系统包版本随软件源更新，不能仅凭基础镜像版本认定工具链完全一致。

## 发布与拉取

ARM64预构建镜像地址为`swr.cn-north-4.myhuaweicloud.com/ubscore/omnistatestore:latest`。私有仓库需先登录；仓库是否允许匿名拉取由维护者配置。维护者更新该镜像时，在ARM64主机执行：

```bash
export IMAGE=swr.cn-north-4.myhuaweicloud.com/ubscore/omnistatestore:latest
docker tag omnistatestore-verify:24.03-lts-sp3-$(uname -m) "$IMAGE"
docker push "$IMAGE"
# 在使用镜像的机器上验证可拉取；私有仓库需先docker login。
docker pull "$IMAGE"
```

若目标SWR仓库拒绝带附加证明的manifest，可在支持这些参数的Buildx版本中使用`docker buildx build --load --provenance=false --sbom=false -f docker/Dockerfile -t "$IMAGE" .`重新构建并推送。该选项用于镜像格式兼容，不是所有仓库的通用要求。

## 开发容器与架构

[开发容器配置](../.devcontainer/devcontainer.json)默认使用上述ARM64镜像。x86_64主机或需要修改依赖时，将`image`替换为`"build": {"dockerfile": "../docker/Dockerfile", "context": ".."}`，通过本地Dockerfile构建。

在目标架构机器上构建并使用镜像。x86_64编译结果不能用于aarch64运行环境；提供的x86_64构建记录不能替代鲲鹏部署验证。24.03构建环境与安装指南中的22.03 LTS SP3基线也有差异，安装前需验证目标环境兼容性。

## 镜像验证记录

2026-09-18发布的ARM64镜像摘要为`sha256:46b647673b9ce152b2e519e26b6f32c125e2acadbfc9cd7e6f1b0e2ff7e1dbdd`。已完成构建、SWR推送、认证拉取和容器启动检查；JNI头文件、ASan与libaio编译运行自检通过。工具版本为GCC 12.3.1、CMake 3.27.9、OpenJDK 1.8.0_502、Maven 3.6.3。

本次使用服务器已有的ARM64 openEuler 24.03 LTS-SP3基础镜像，通过`BASE_IMAGE`参数指定。四个Flink版本的依赖预热均在解析项目自身尚未构建的`flink-boost-statestore-api`模块时报告缓存不完整，已下载的外部依赖保留在镜像中。未在该镜像内执行完整项目Release构建、UT或Flink作业验证。
