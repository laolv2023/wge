#!/usr/bin/env bash
# =============================================================================
# WGE-Kafka Detector — 构建脚本
#
# 用途:
#   检查依赖、配置 CMake、编译项目、可选运行测试和安装。
#
# 用法:
#   ./scripts/build.sh [OPTIONS]
#
# 选项:
#   --debug             Debug 构建
#   --release           Release 构建（默认）
#   --relwithdebinfo    Release + Debug Info 构建
#   --test              编译后运行测试
#   --install           编译后安装到系统
#   --clean             清理后重新构建
#   -j N                并行编译线程数（默认: nproc）
#   -h, --help          显示帮助
#
# 环境变量:
#   VCPKG_ROOT          vcpkg 安装路径（默认: /opt/vcpkg）
#   CMAKE_TOOLCHAIN_FILE CMake 工具链文件（默认: $VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake）
#   WGE_SDK_PATH        WGE SDK 安装路径（默认: /opt/wge）
#   BUILD_DIR           CMake 构建目录（默认: build）
#   INSTALL_PREFIX      安装前缀（默认: /usr/local）
# =============================================================================

set -euo pipefail

# ============================================================================
# 颜色定义
# ============================================================================
readonly COLOR_RESET='\033[0m'
readonly COLOR_GREEN='\033[0;32m'
readonly COLOR_RED='\033[0;31m'
readonly COLOR_YELLOW='\033[1;33m'
readonly COLOR_CYAN='\033[0;36m'
readonly COLOR_BOLD='\033[1m'

# ============================================================================
# 默认值
# ============================================================================
BUILD_TYPE="Release"
RUN_TESTS=false
DO_INSTALL=false
CLEAN_BUILD=false
NUM_JOBS=$(nproc 2>/dev/null || echo 4)
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# 可配置路径
VCPKG_ROOT="${VCPKG_ROOT:-/opt/vcpkg}"
CMAKE_TOOLCHAIN_FILE="${CMAKE_TOOLCHAIN_FILE:-${VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake}"
WGE_SDK_PATH="${WGE_SDK_PATH:-/opt/wge}"
BUILD_DIR="${BUILD_DIR:-${PROJECT_ROOT}/build}"
INSTALL_PREFIX="${INSTALL_PREFIX:-/usr/local}"

# ============================================================================
# 辅助函数
# ============================================================================

print_step() {
    echo -e "${COLOR_CYAN}==>${COLOR_RESET} ${COLOR_BOLD}$*${COLOR_RESET}"
}

print_ok() {
    echo -e "    ${COLOR_GREEN}✓${COLOR_RESET} $*"
}

print_warn() {
    echo -e "    ${COLOR_YELLOW}⚠${COLOR_RESET} $*"
}

print_err() {
    echo -e "    ${COLOR_RED}✗${COLOR_RESET} $*"
}

print_header() {
    echo ""
    echo -e "${COLOR_BOLD}${COLOR_CYAN}══════════════════════════════════════════════════════════════${COLOR_RESET}"
    echo -e "${COLOR_BOLD}${COLOR_CYAN}  WGE-Kafka Detector — Build Script${COLOR_RESET}"
    echo -e "${COLOR_BOLD}${COLOR_CYAN}══════════════════════════════════════════════════════════════${COLOR_RESET}"
    echo ""
}

show_help() {
    cat <<EOF
Usage: $0 [OPTIONS]

Options:
  --debug             Debug build
  --release           Release build (default)
  --relwithdebinfo    Release with debug info
  --test              编译后运行测试
  --install           编译后安装到系统
  --clean             清理后重新构建
  -j N                并行编译线程数 (default: $(nproc 2>/dev/null || echo 4))
  -h, --help          显示帮助

Environment Variables:
  VCPKG_ROOT          vcpkg installation path (default: /opt/vcpkg)
  CMAKE_TOOLCHAIN_FILE CMake toolchain file
  WGE_SDK_PATH        WGE SDK path (default: /opt/wge)
  BUILD_DIR           CMake build directory (default: ./build)
  INSTALL_PREFIX      Installation prefix (default: /usr/local)

Examples:
  $0                                    # Release 构建
  $0 --debug --test                     # Debug 构建 + 运行测试
  $0 --release --install                # Release 构建 + 安装
  $0 --clean --relwithdebinfo -j 8      # 清理后 RelWithDebInfo 构建，8 线程
EOF
}

# ============================================================================
# 参数解析
# ============================================================================
while [[ $# -gt 0 ]]; do
    case "$1" in
        --debug)
            BUILD_TYPE="Debug"
            shift
            ;;
        --release)
            BUILD_TYPE="Release"
            shift
            ;;
        --relwithdebinfo)
            BUILD_TYPE="RelWithDebInfo"
            shift
            ;;
        --test)
            RUN_TESTS=true
            shift
            ;;
        --install)
            DO_INSTALL=true
            shift
            ;;
        --clean)
            CLEAN_BUILD=true
            shift
            ;;
        -j)
            if [[ $# -lt 2 ]]; then
                print_err "Missing value for -j"
                exit 1
            fi
            NUM_JOBS="$2"
            shift 2
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        *)
            print_err "Unknown option: $1"
            echo "Run '$0 --help' for usage."
            exit 1
            ;;
    esac
done

# ============================================================================
# 依赖检查
# ============================================================================

check_dependency() {
    local name="$1"
    local check_cmd="$2"
    local required="${3:-true}"

    if eval "$check_cmd" &>/dev/null; then
        print_ok "$name found"
        return 0
    else
        if [[ "$required" == "true" ]]; then
            print_err "$name NOT found (required)"
            return 1
        else
            print_warn "$name NOT found (optional, skipped)"
            return 0
        fi
    fi
}

check_dependencies() {
    print_step "检查构建依赖..."

    local deps_ok=true

    # CMake >= 3.28
    if command -v cmake &>/dev/null; then
        local cmake_ver
        cmake_ver=$(cmake --version 2>/dev/null | head -1 | grep -oP '\d+\.\d+' | head -1)
        if [[ -n "$cmake_ver" ]]; then
            if printf '%s\n' "3.28" "$cmake_ver" | sort -V -C 2>/dev/null; then
                print_ok "CMake ${cmake_ver} (>= 3.28)"
            else
                print_err "CMake ${cmake_ver} < 3.28 required"
                print_warn "Install via: https://apt.kitware.com/ (Ubuntu) or dnf install cmake (RHEL)"
                deps_ok=false
            fi
        else
            print_err "Cannot determine CMake version"
            deps_ok=false
        fi
    else
        print_err "CMake not found (required)"
        deps_ok=false
    fi

    # GCC >= 13.1
    if command -v g++ &>/dev/null; then
        local gcc_ver
        gcc_ver=$(g++ -dumpversion 2>/dev/null)
        if [[ -n "$gcc_ver" ]]; then
            if printf '%s\n' "13.1" "$gcc_ver" | sort -V -C 2>/dev/null; then
                print_ok "GCC ${gcc_ver} (>= 13.1)"
            else
                print_err "GCC ${gcc_ver} < 13.1 required"
                print_warn "Install GCC 13+: sudo apt install gcc-13 g++-13 (Ubuntu)"
                deps_ok=false
            fi
        else
            print_err "Cannot determine GCC version"
            deps_ok=false
        fi
    else
        print_err "GCC/g++ not found (required)"
        deps_ok=false
    fi

    # vcpkg
    if [[ -f "$CMAKE_TOOLCHAIN_FILE" ]]; then
        print_ok "vcpkg toolchain: ${CMAKE_TOOLCHAIN_FILE}"
    else
        print_err "vcpkg toolchain not found: ${CMAKE_TOOLCHAIN_FILE}"
        print_warn "Set VCPKG_ROOT or CMAKE_TOOLCHAIN_FILE environment variable"
        print_warn "Install: git clone https://github.com/microsoft/vcpkg.git /opt/vcpkg && cd /opt/vcpkg && ./bootstrap-vcpkg.sh"
        deps_ok=false
    fi

    # WGE SDK
    if [[ -d "$WGE_SDK_PATH" ]] && [[ -f "${WGE_SDK_PATH}/include/wge/engine.h" ]]; then
        print_ok "WGE SDK: ${WGE_SDK_PATH}"
    else
        print_err "WGE SDK not found: ${WGE_SDK_PATH}"
        print_warn "Set WGE_SDK_PATH to your WGE SDK installation directory"
        deps_ok=false
    fi

    # vcpkg installed libraries
    local vcpkg_installed="${VCPKG_ROOT}/installed/x64-linux"
    if [[ -d "$vcpkg_installed" ]]; then
        check_dependency "librdkafka"   "ls ${vcpkg_installed}/include/librdkafka/rdkafkacpp.h" true || deps_ok=false
        check_dependency "protobuf"     "ls ${vcpkg_installed}/include/google/protobuf/message.h" true || deps_ok=false
        check_dependency "simdjson"     "ls ${vcpkg_installed}/include/simdjson.h" true || deps_ok=false
        check_dependency "re2"          "ls ${vcpkg_installed}/include/re2/re2.h" true || deps_ok=false
        check_dependency "yaml-cpp"     "ls ${vcpkg_installed}/include/yaml-cpp/yaml.h" true || deps_ok=false
        check_dependency "spdlog"       "ls ${vcpkg_installed}/include/spdlog/spdlog.h" true || deps_ok=false
        check_dependency "prometheus-cpp" "ls ${vcpkg_installed}/include/prometheus/registry.h 2>/dev/null" false || true
        check_dependency "gtest"        "ls ${vcpkg_installed}/include/gtest/gtest.h 2>/dev/null" false || true
    else
        print_warn "vcpkg installed directory not found: ${vcpkg_installed}"
        print_warn "Skipping library checks. Run: vcpkg install librdkafka protobuf simdjson re2 yaml-cpp spdlog prometheus-cpp gtest"
    fi

    if ! $deps_ok; then
        echo ""
        print_err "Dependency check FAILED. Please fix the above issues before building."
        exit 1
    fi

    echo ""
    print_ok "All required dependencies satisfied."
}

# ============================================================================
# 构建
# ============================================================================

do_build() {
    local cmake_flags=(
        "-DCMAKE_BUILD_TYPE=${BUILD_TYPE}"
        "-DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE}"
        "-DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX}"
        "-DWGE_SDK_PATH=${WGE_SDK_PATH}"
    )

    # Enable tests in Debug mode by default, or when --test is set
    if [[ "$BUILD_TYPE" == "Debug" ]] || $RUN_TESTS; then
        cmake_flags+=("-DWGE_DETECTOR_ENABLE_TESTS=ON")
    fi

    # Clean if requested
    if $CLEAN_BUILD; then
        print_step "清理构建目录..."
        rm -rf "$BUILD_DIR"
        print_ok "构建目录已清理"
    fi

    # 配置
    print_step "CMake 配置 (${BUILD_TYPE})..."
    mkdir -p "$BUILD_DIR"

    if ! cmake -S "$PROJECT_ROOT" -B "$BUILD_DIR" "${cmake_flags[@]}"; then
        echo ""
        print_err "CMake 配置失败"
        echo ""
        print_step "诊断信息:"
        echo "  构建类型:   ${BUILD_TYPE}"
        echo "  项目根目录: ${PROJECT_ROOT}"
        echo "  构建目录:   ${BUILD_DIR}"
        echo "  Toolchain:  ${CMAKE_TOOLCHAIN_FILE}"
        echo "  WGE SDK:    ${WGE_SDK_PATH}"
        echo ""
        if [[ -f "${BUILD_DIR}/CMakeFiles/CMakeError.log" ]]; then
            echo "  --- CMakeError.log (最后 40 行) ---"
            tail -40 "${BUILD_DIR}/CMakeFiles/CMakeError.log" 2>/dev/null || true
        fi
        exit 1
    fi
    print_ok "CMake 配置完成"

    # 编译
    print_step "编译 (${BUILD_TYPE}, ${NUM_JOBS} 线程)..."
    local build_start
    build_start=$(date +%s)

    if ! cmake --build "$BUILD_DIR" -j "$NUM_JOBS"; then
        local build_end
        build_end=$(date +%s)
        echo ""
        print_err "编译失败 (耗时 $((build_end - build_start))s)"
        echo ""
        print_step "诊断信息:"
        echo "  查看完整编译日志: cat ${BUILD_DIR}/compile_commands.json"
        echo "  建议: 尝试单线程编译以查看完整错误: cmake --build ${BUILD_DIR} -j1"
        exit 1
    fi

    local build_end
    build_end=$(date +%s)
    print_ok "编译完成 (耗时 $((build_end - build_start))s)"

    # 验证二进制文件
    if [[ -f "${BUILD_DIR}/wge-detector" ]]; then
        local bin_size
        bin_size=$(stat --printf="%s" "${BUILD_DIR}/wge-detector" 2>/dev/null || stat -f%z "${BUILD_DIR}/wge-detector" 2>/dev/null)
        print_ok "二进制文件: ${BUILD_DIR}/wge-detector ($(( bin_size / 1024 )) KB)"
    fi
}

# ============================================================================
# 测试
# ============================================================================

do_test() {
    if [[ ! -f "${BUILD_DIR}/wge_detector_test" ]]; then
        print_warn "测试可执行文件不存在: ${BUILD_DIR}/wge_detector_test"
        print_warn "请确保使用 --test 参数或 Debug 模式构建以启用测试"
        return 0
    fi

    print_step "运行单元测试..."
    cd "$BUILD_DIR"

    if ctest --output-on-failure --test-dir "$BUILD_DIR" 2>&1; then
        print_ok "所有测试通过"
    else
        local ctest_exit=$?
        print_err "部分测试失败 (exit code: ${ctest_exit})"
        echo ""
        print_step "诊断信息:"
        echo "  查看详细测试日志: cd ${BUILD_DIR} && ctest --output-on-failure"
        echo "  单独运行: ${BUILD_DIR}/wge_detector_test"
        exit $ctest_exit
    fi
}

# ============================================================================
# 安装
# ============================================================================

do_install() {
    if [[ ! -f "${BUILD_DIR}/wge-detector" ]]; then
        print_err "二进制文件不存在，请先编译"
        exit 1
    fi

    print_step "安装到 ${INSTALL_PREFIX}..."
    if ! cmake --install "$BUILD_DIR" --prefix "$INSTALL_PREFIX"; then
        print_err "安装失败（可能需要 sudo 权限）"
        print_warn "请尝试: sudo $0 --install"
        exit 1
    fi
    print_ok "安装完成: ${INSTALL_PREFIX}/bin/wge-detector"
}

# ============================================================================
# 主流程
# ============================================================================

main() {
    print_header

    echo -e "  构建类型:     ${COLOR_BOLD}${BUILD_TYPE}${COLOR_RESET}"
    echo -e "  并行线程:     ${COLOR_BOLD}${NUM_JOBS}${COLOR_RESET}"
    echo -e "  运行测试:     ${COLOR_BOLD}${RUN_TESTS}${COLOR_RESET}"
    echo -e "  安装到系统:   ${COLOR_BOLD}${DO_INSTALL}${COLOR_RESET}"
    echo -e "  清理构建:     ${COLOR_BOLD}${CLEAN_BUILD}${COLOR_RESET}"
    echo -e "  构建目录:     ${COLOR_BOLD}${BUILD_DIR}${COLOR_RESET}"
    echo -e "  vcpkg:        ${COLOR_BOLD}${VCPKG_ROOT}${COLOR_RESET}"
    echo ""

    check_dependencies
    do_build

    if $RUN_TESTS; then
        do_test
    fi

    if $DO_INSTALL; then
        do_install
    fi

    echo ""
    echo -e "${COLOR_GREEN}══════════════════════════════════════════════════════════════${COLOR_RESET}"
    echo -e "${COLOR_GREEN}  构建成功完成!${COLOR_RESET}"
    echo -e "${COLOR_GREEN}══════════════════════════════════════════════════════════════${COLOR_RESET}"
    echo ""
    echo -e "  运行:     ${BUILD_DIR}/wge-detector --config /etc/wge/wge-detector.yaml"
    echo -e "  安装:     sudo $0 --install"
    echo -e "  测试:     cd ${BUILD_DIR} && ctest"
    echo ""
}

main "$@"
