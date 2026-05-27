# =============================================================================
# WGE-Kafka Detector — Dockerfile (多阶段构建)
#
# 用途:
#   在 Docker 容器中构建并运行 WGE-Kafka Detector。
#
# 阶段:
#   1. builder:  安装所有构建依赖，编译项目
#   2. runtime:  仅复制运行时依赖和二进制文件，生成最小运行镜像
#
# 构建:
#   docker build -t wge-kafka-detector:latest .
#
# 运行:
#   docker run -d \
#     -v /path/to/wge-detector.yaml:/etc/wge/wge-detector.yaml:ro \
#     -v /path/to/log_mapping.yaml:/etc/wge/log_mapping.yaml:ro \
#     -v /path/to/wge-rules:/opt/wge/rules:ro \
#     -p 9100:9100 \
#     -p 9101:9101 \
#     wge-kafka-detector:latest
#
# 环境变量:
#   BUILD_TYPE         构建类型 (Release|Debug|RelWithDebInfo, 默认 Release)
#   BUILD_JOBS         并行编译线程数 (默认: nproc)
#   WGE_SDK_URL        WGE SDK 下载地址（企业许可）
#   WGE_SDK_CHECKSUM   WGE SDK 校验和
# =============================================================================

# ============================================================================
# 阶段 1: Builder
# ============================================================================
FROM ubuntu:24.04 AS builder

ARG BUILD_TYPE=Release
ARG BUILD_JOBS=4
ARG DEBIAN_FRONTEND=noninteractive

# ---- 安装系统构建依赖 ----
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc-14 g++-14 \
    cmake \
    pkg-config \
    git \
    curl \
    ca-certificates \
    zip \
    unzip \
    tar \
    ninja-build \
    && update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-14 100 \
    && update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-14 100 \
    && update-alternatives --install /usr/bin/cpp cpp /usr/bin/cpp-14 100 \
    && rm -rf /var/lib/apt/lists/*

# ---- 安装 vcpkg ----
WORKDIR /opt
RUN git clone --depth 1 https://github.com/microsoft/vcpkg.git vcpkg \
    && cd vcpkg \
    && ./bootstrap-vcpkg.sh -disableMetrics

ENV VCPKG_ROOT=/opt/vcpkg
ENV CMAKE_TOOLCHAIN_FILE=${VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake

# ---- 安装 C++ 依赖 ----
RUN ${VCPKG_ROOT}/vcpkg install \
    librdkafka \
    protobuf \
    simdjson \
    re2 \
    yaml-cpp \
    spdlog \
    prometheus-cpp \
    gtest

# ---- 安装 WGE SDK (需要企业许可) ----
# 以下为占位示例。实际构建时需提供 WGE SDK 的下载方式。
# 方式 A: 从私有 Artifactory/HTTP 下载
# ARG WGE_SDK_URL
# ARG WGE_SDK_CHECKSUM
# RUN curl -fsSL -o /tmp/wge-sdk.tar.gz "${WGE_SDK_URL}" \
#     && echo "${WGE_SDK_CHECKSUM}  /tmp/wge-sdk.tar.gz" | sha256sum -c - \
#     && mkdir -p /opt/wge \
#     && tar -xzf /tmp/wge-sdk.tar.gz -C /opt/wge --strip-components=1 \
#     && rm /tmp/wge-sdk.tar.gz
#
# 方式 B: 从构建上下文复制
# COPY wge-sdk/ /opt/wge/

# 为无 WGE SDK 的构建提供占位目录（实际部署时必须替换）
RUN mkdir -p /opt/wge/include/wge /opt/wge/lib

ENV WGE_SDK_PATH=/opt/wge

# ---- 复制源码 ----
WORKDIR /src
COPY CMakeLists.txt .
COPY proto/ proto/
COPY src/ src/
COPY test/ test/

# ---- CMake 配置 ----
RUN cmake -B build \
    -DCMAKE_BUILD_TYPE=${BUILD_TYPE} \
    -DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE} \
    -DWGE_SDK_PATH=${WGE_SDK_PATH} \
    -DWGE_DETECTOR_ENABLE_TESTS=OFF \
    -DWGE_DETECTOR_ENABLE_INTEGRATION_TESTS=OFF \
    -DCMAKE_INSTALL_PREFIX=/usr/local

# ---- 编译 ----
RUN cmake --build build -j ${BUILD_JOBS}

# ---- 安装到临时目录 ----
RUN cmake --install build --prefix /tmp/install

# ============================================================================
# 阶段 2: Runtime
# ============================================================================
FROM ubuntu:24.04-slim AS runtime

ARG DEBIAN_FRONTEND=noninteractive

# ---- 安装运行时依赖 ----
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

# ---- 从 vcpkg 复制运行时 .so ----
COPY --from=builder /opt/vcpkg/installed/x64-linux/lib/ /usr/local/lib/
COPY --from=builder /opt/vcpkg/installed/x64-linux/lib/x64-linux/ /usr/local/lib/ 2>/dev/null || true

# ---- 复制 WGE SDK 运行时库 ----
COPY --from=builder /opt/wge/lib/ /usr/local/lib/ 2>/dev/null || true

# ---- 配置动态链接器 ----
RUN echo "/usr/local/lib" > /etc/ld.so.conf.d/wge-detector.conf \
    && ldconfig

# ---- 复制二进制文件 ----
COPY --from=builder /tmp/install/bin/wge-detector /usr/local/bin/wge-detector

# ---- 创建运行时用户和目录 ----
RUN groupadd --system wge \
    && useradd --system --no-create-home --shell /usr/sbin/nologin \
       --home-dir /var/lib/wge -g wge wge \
    && mkdir -p /etc/wge /var/log/wge /var/lib/wge/wal \
    && chown -R wge:wge /etc/wge /var/log/wge /var/lib/wge

# ---- 复制默认配置（如果存在） ----
COPY config/ /etc/wge/ 2>/dev/null || true

# ---- 暴露端口 ----
# 9100: Health check endpoint
# 9101: Prometheus metrics endpoint
EXPOSE 9100 9101

# ---- 元数据 ----
LABEL org.opencontainers.image.title="WGE-Kafka Detector"
LABEL org.opencontainers.image.description="Kafka-WGE security detection bridge service"
LABEL org.opencontainers.image.version="0.1.0"
LABEL org.opencontainers.image.source="https://github.com/laolv2023/wge"
LABEL org.opencontainers.image.licenses="Proprietary"

# ---- 健康检查 ----
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:9100/health || exit 1

# ---- 运行时配置 ----
USER wge
WORKDIR /var/lib/wge

ENTRYPOINT ["/usr/local/bin/wge-detector"]
CMD ["--config", "/etc/wge/wge-detector.yaml"]
