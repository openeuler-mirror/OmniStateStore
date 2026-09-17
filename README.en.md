# OmniStateStore

<!-- markdownlint-disable MD001 MD012 MD022 MD030 -->

#### Description
OmniStateStore is an open-source and high-performance state storage engine for Flink.

#### Software Architecture
Software architecture description

#### Installation

1.  xxxx
2.  xxxx
3.  xxxx

#### Instructions

1.  xxxx
2.  xxxx
3.  xxxx

#### Public Network Address Declaration

This declaration covers public network addresses in this repository's own source code, build configuration, and documentation. It excludes addresses in the `3rdparty` directory and in other third-party component source files that carry third-party license notices. OmniStateStore does not actively access the following public network addresses at runtime. Except for Git submodule initialization and build dependency retrieval, these addresses are used only as license identifiers or XML namespaces.

| Type | Public network address | Location | Purpose |
|--|--|--|--|
| License | [http://license.coscl.org.cn/MulanPSL2](http://license.coscl.org.cn/MulanPSL2) | `LICENSE`, and source files under `src` and `test` | Identifies the Mulan Permissive Software License v2 and does not initiate network access. |
| Third-party dependency retrieval | <ul><li><a href="https://gitcode.com/GitHub_Trending/go/googletest.git">GoogleTest</a>: a C++ unit testing framework used to build and run tests.</li><li><a href="https://gitcode.com/GitHub_Trending/lz/lz4.git">LZ4</a>: a lossless compression library used for data compression and decompression.</li><li><a href="https://gitcode.com/src-openeuler/libboundscheck.git">libboundscheck</a>: a secure function library that provides bounds-checked memory and string operations.</li><li><a href="https://gitcode.com/GitHub_Trending/sp/spdlog.git">spdlog</a>: a C++ logging library used by native modules.</li></ul> | `.gitmodules` | Retrieves build dependencies when Git submodules are initialized or updated; these addresses are not accessed at runtime. |
| Build metadata | <ul><li><a href="http://maven.apache.org/xsd/maven-4.0.0.xsd">http://maven.apache.org/xsd/maven-4.0.0.xsd</a></li><li><a href="https://maven.apache.org/xsd/assembly-2.2.0.xsd">https://maven.apache.org/xsd/assembly-2.2.0.xsd</a></li><li><a href="http://www.w3.org/2001/XMLSchema-instance">http://www.w3.org/2001/XMLSchema-instance</a></li></ul> | `src/plugin/pom.xml`, `src/plugin/state_store_api/pom.xml`, `src/plugin/state_store_plugin/pom.xml`, and `src/plugin/assembly.xml` | XML namespace and schema identifiers in Maven POM and Assembly files; they are not runtime access addresses. |

#### Contribution

1.  Fork the repository
2.  Create Feat_xxx branch
3.  Commit your code
4.  Create Pull Request


#### Gitee Feature

1.  You can use Readme\_XXX.md to support different languages, such as Readme\_en.md, Readme\_zh.md
2.  Gitee blog `https://blog.gitee.com`
3.  Explore open source project [https://gitee.com/explore](https://gitee.com/explore)
4.  The most valuable open source project [GVP](https://gitee.com/gvp)
5.  The manual of Gitee `https://gitee.com/help`
6.  The most popular members  [https://gitee.com/gitee-stars/](https://gitee.com/gitee-stars/)
