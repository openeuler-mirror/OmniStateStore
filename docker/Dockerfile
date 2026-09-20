# Build and UT dependencies; mount the repository at /workspace when running.
ARG BASE_IMAGE=hub.oepkgs.net/openeuler/openeuler:24.03-lts-sp3
FROM ${BASE_IMAGE}

ARG PREWARM_MAVEN=1
ARG MAVEN_REPO_REF=master
ENV LANG=C.UTF-8

RUN set -eux; \
    dnf install -y gcc gcc-c++ make cmake java-1.8.0-openjdk-devel \
        maven git libaio-devel libasan tar gzip which findutils \
        gawk sed grep coreutils diffutils; \
    dnf clean all; \
    ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v javac)")")")" /opt/java; \
    mkdir -p /root/.m2/repository

ENV JAVA_HOME=/opt/java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Best-effort cache warming. Reactor dependencies may still need downloading
# during the actual build. No repository source is retained in this layer.
RUN set -eux; \
    case "${PREWARM_MAVEN}" in 0|1) ;; *) exit 1 ;; esac; \
    if [ "${PREWARM_MAVEN}" = "1" ]; then \
        if git clone --depth 1 --filter=blob:none --sparse \
            --branch "${MAVEN_REPO_REF}" \
            https://atomgit.com/openeuler/OmniStateStore.git /tmp/omn; then \
            cd /tmp/omn; \
            git sparse-checkout set src/plugin; \
            cd src/plugin; \
            for v in 1.16.1 1.16.3 1.17.1 1.20.0; do \
                mvn -B dependency:go-offline -Drevision=1.1.0-SNAPSHOT \
                    -Pflink-${v} \
                    || echo "WARNING: incomplete cache for flink-${v}; build requires network"; \
            done; \
        else \
            echo "WARNING: cache warming skipped; build requires network"; \
        fi; \
        cd /; \
        rm -rf /tmp/omn; \
    fi

# Dependency smoke checks, not project UT verification.
RUN set -eux; \
    gcc --version; cmake --version; java -version; mvn -v; \
    test -f "${JAVA_HOME}/include/jni.h"; \
    printf '#include <libaio.h>\nint main(){return 0;}\n' > /tmp/omn-check.cpp; \
    g++ -fsanitize=address /tmp/omn-check.cpp -laio -o /tmp/omn-check; \
    /tmp/omn-check; \
    rm -f /tmp/omn-check.cpp /tmp/omn-check

WORKDIR /workspace
CMD ["sleep", "infinity"]
