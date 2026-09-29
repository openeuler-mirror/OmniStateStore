# OmniStateStore编译与测试环境镜像

本目录提供预装工具链和UT依赖的环境镜像。镜像不包含项目源码、编译产物或Flink，启动后挂载源码并执行项目构建脚本。完整流程见[容器环境部署](../docs/zh/installation_guide.md#容器环境部署)。

## 镜像内容

| 项目 | 内容 |
| --- | --- |
| 基础系统 | openEuler 24.03 LTS-SP3 |
| 工具链 | GCC/G++、CMake、Make、Git、mold并行链接器（通过`LDFLAGS`用于新配置的CMake构建） |
| Java | OpenJDK 8开发包、Maven；设置JAVA_HOME |
| UT依赖 | libaio-devel、libasan |
| Maven缓存 | 使用Maven Central镜像源，对Flink 1.16.1、1.16.3、1.17.1、1.20.0分别执行打包以预取依赖和构建插件 |

默认使用`https://repo.huaweicloud.com/repository/maven/`作为Maven Central镜像源，可通过`MAVEN_MIRROR_URL`构建参数修改。预热默认开启；任一Flink版本打包失败会使镜像构建失败，避免发布缓存不完整的镜像。预热使用上游master，可通过`MAVEN_REPO_REF`指定分支或标签；应将其设为待编译源码对应的分支或标签，以提高缓存命中率。缓存不会覆盖运行时挂载的源码。预热完成后，源码更新或新增依赖仍可能需要联网。Dockerfile中的自检只检查工具链、JNI头文件及mold/ASan/libaio链接运行，不代表项目UT通过。

## 构建

在Linux主机的仓库根目录执行：

```bash
docker build -f docker/Dockerfile \
    -t omnistatestore-verify:24.03-lts-sp3-$(uname -m) .
```

`--build-arg PREWARM_MAVEN=0`关闭依赖预热。`--build-arg MAVEN_MIRROR_URL=https://repo.maven.apache.org/maven2/`使用Maven Central原站。`--build-arg BASE_IMAGE=hub.oepkgs.net/openeuler/openeuler:22.03-lts-sp3`切换到22.03基线，切换后需验证软件源是否提供mold及GCC是否支持`-fuse-ld=mold`（需GCC 12.1及以上），必要时调整链接器配置，并使用相应镜像标签。系统包版本随软件源更新，不能仅凭基础镜像版本认定工具链完全一致。

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

本次使用服务器已有的ARM64 openEuler 24.03 LTS-SP3基础镜像，通过`BASE_IMAGE`参数指定。四个Flink版本的依赖预热均在解析项目自身尚未构建的`flink-boost-statestore-api`模块时报告缓存不完整，已下载的外部依赖保留在镜像中。该记录对应旧版镜像，尚未包含本次mold及Maven预热改动。未在该镜像内执行完整项目Release构建、UT或Flink作业验证。

2026-09-29在ARM64主机上按当前Dockerfile构建临时验证镜像，四个Flink版本的Maven预热均完成打包。镜像内mold 2.34.1、JNI头文件检查，以及使用mold链接的ASan/libaio程序编译运行自检通过。此镜像仅用于本地验证，未发布；尚未在新镜像内执行完整项目UT或测量`bss_ut`的链接耗时。

同日在该ARM64主机按[容器环境部署](../docs/zh/installation_guide.md#容器环境部署)的流程验证完整Release打包。以`0fadbd48`源码及其四个子模块为输入，镜像构建增加`--no-cache`以测量无层缓存耗时：镜像构建228秒，源码和子模块准备8秒，容器启动不足1秒，容器内执行`bash scripts/build.sh -t release`为417秒，分段合计约653秒（10分53秒）。最终生成`BoostKit-omnistatestore_1.1.0_aarch64_release.tar.gz`，包含四个受支持Flink版本的JAR。计时不包含基础镜像首次拉取；使用了镜像内已预热的Maven依赖。
