# Build and UT dependencies; mount the repository at /workspace when running.
ARG BASE_IMAGE=hub.oepkgs.net/openeuler/openeuler:24.03-lts-sp3
FROM ${BASE_IMAGE}

ARG PREWARM_MAVEN=1
ARG MAVEN_REPO_REF=master
ARG MAVEN_MIRROR_URL=https://repo.huaweicloud.com/repository/maven/
ENV LANG=C.UTF-8

RUN set -eux; \
    dnf install -y gcc gcc-c++ make cmake java-1.8.0-openjdk-devel \
        maven git libaio-devel libasan tar gzip which findutils \
        gawk sed grep coreutils diffutils mold; \
    dnf clean all; \
    ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v javac)")")")" /opt/java; \
    mkdir -p /root/.m2/repository

ENV JAVA_HOME=/opt/java
ENV PATH="${JAVA_HOME}/bin:${PATH}"
ENV LDFLAGS="-fuse-ld=mold"

# Mirror only Maven Central. Override MAVEN_MIRROR_URL when another mirror is
# needed; Maven's build plugins also use this setting.
RUN set -eux; \
    printf '%s\n' \
        '<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0">' \
        '  <mirrors><mirror>' \
        '    <id>central-mirror</id><mirrorOf>central</mirrorOf>' \
        "    <url>${MAVEN_MIRROR_URL}</url>" \
        '  </mirror></mirrors>' \
        '</settings>' > /root/.m2/settings.xml

# Run the same Maven lifecycle as release builds to resolve reactor modules,
# build plugins, and their transitive dependencies. No source is retained.
RUN set -eux; \
    case "${PREWARM_MAVEN}" in 0|1) ;; *) exit 1 ;; esac; \
    if [ "${PREWARM_MAVEN}" = "1" ]; then \
        git clone --depth 1 --filter=blob:none --sparse \
            --branch "${MAVEN_REPO_REF}" \
            https://atomgit.com/openeuler/OmniStateStore.git /tmp/omn; \
        cd /tmp/omn; \
        git sparse-checkout set src/plugin; \
        cd src/plugin; \
        for v in 1.16.1 1.16.3 1.17.1 1.20.0; do \
            mvn -B -ntp clean package -Drevision=1.1.0-SNAPSHOT \
                -Dmaven.test.skip=true -Dmaven.compiler.fork=true \
                -Pflink-${v}; \
        done; \
        cd /; \
        rm -rf /tmp/omn; \
    fi

# Dependency smoke checks, not project UT verification.
RUN set -eux; \
    gcc --version; cmake --version; java -version; mvn -v; mold --version; \
    test -f "${JAVA_HOME}/include/jni.h"; \
    printf '#include <libaio.h>\nint main(){return 0;}\n' > /tmp/omn-check.cpp; \
    g++ -fuse-ld=mold -fsanitize=address /tmp/omn-check.cpp -laio -o /tmp/omn-check; \
    /tmp/omn-check; \
    rm -f /tmp/omn-check.cpp /tmp/omn-check

WORKDIR /workspace
CMD ["sleep", "infinity"]
