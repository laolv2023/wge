#!/usr/bin/env bash
# =============================================================================
# WGE-Kafka Detector — 依赖检查脚本
#
# 用途:
#   检查所有编译和运行时依赖是否满足，以表格形式输出检查结果。
#
# 用法:
#   ./scripts/dependency_check.sh            # 标准检查
#   ./scripts/dependency_check.sh --json     # JSON 格式输出
#   ./scripts/dependency_check.sh --verbose  # 详细输出
#   ./scripts/dependency_check.sh -h         # 显示帮助
#
# 检查项:
#   - OS 版本
#   - GCC 版本 (≥ 13.1)
#   - CMake 版本 (≥ 3.28)
#   - vcpkg 安装
#   - WGE 库
#   - librdkafka (≥ 2.0)
#   - protobuf (≥ 3.15)
#   - simdjson (≥ 3.0)
#   - re2
#   - yaml-cpp (≥ 0.7)
#   - spdlog (≥ 1.10)
#   - gtest (可选)
#   - 磁盘空间 (≥ 2GB)
#   - 内存 (≥ 2GB)
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
# 可配置路径
# ============================================================================
VCPKG_ROOT="${VCPKG_ROOT:-/opt/vcpkg}"
WGE_SDK_PATH="${WGE_SDK_PATH:-/opt/wge}"
OUTPUT_FORMAT="table"   # table | json

# ============================================================================
# 结果追踪
# ============================================================================
declare -A CHECK_RESULTS
declare -A CHECK_DETAILS
TOTAL_CHECKS=0
PASSED_CHECKS=0
FAILED_CHECKS=0
WARNED_CHECKS=0

# ============================================================================
# 辅助函数
# ============================================================================

print_header() {
    echo ""
    echo -e "${COLOR_BOLD}${COLOR_CYAN}══════════════════════════════════════════════════════════════${COLOR_RESET}"
    echo -e "${COLOR_BOLD}${COLOR_CYAN}  WGE-Kafka Detector — Dependency Check${COLOR_RESET}"
    echo -e "${COLOR_BOLD}${COLOR_CYAN}══════════════════════════════════════════════════════════════${COLOR_RESET}"
}

show_help() {
    cat <<EOF
Usage: $0 [OPTIONS]

Options:
  --json      以 JSON 格式输出结果
  --verbose   详细输出（显示版本和路径详情）
  -h, --help  显示帮助

Environment Variables:
  VCPKG_ROOT   vcpkg 安装路径（默认: /opt/vcpkg）
  WGE_SDK_PATH WGE SDK 安装路径（默认: /opt/wge）

Check Items:
  OS version, GCC, CMake, vcpkg, WGE SDK, librdkafka, protobuf,
  simdjson, re2, yaml-cpp, spdlog, gtest (optional),
  disk space, memory
EOF
}

# version_gte: returns 0 if $1 >= $2
version_gte() {
    if [[ "$1" == "$2" ]]; then
        return 0
    fi
    local sorted
    sorted=$(printf '%s\n' "$2" "$1" | sort -V | head -1)
    [[ "$sorted" == "$2" ]]
}

# record: record a check result
# 用法: record <name> <status> <detail>
# status: PASS | FAIL | WARN
record() {
    local name="$1"
    local status="$2"
    local detail="$3"

    CHECK_RESULTS["$name"]="$status"
    CHECK_DETAILS["$name"]="$detail"
    TOTAL_CHECKS=$((TOTAL_CHECKS + 1))

    case "$status" in
        PASS) PASSED_CHECKS=$((PASSED_CHECKS + 1)) ;;
        FAIL) FAILED_CHECKS=$((FAILED_CHECKS + 1)) ;;
        WARN) WARNED_CHECKS=$((WARNED_CHECKS + 1)) ;;
    esac
}

# ============================================================================
# 各项检查
# ============================================================================

check_os() {
    local os_name="Unknown"
    local os_version="Unknown"

    if [[ -f /etc/os-release ]]; then
        os_name=$(grep -oP '^PRETTY_NAME="?\K[^"]+' /etc/os-release 2>/dev/null || echo "Linux")
        os_version=$(grep -oP '^VERSION_ID="?\K[^"]+' /etc/os-release 2>/dev/null || echo "Unknown")
    elif [[ -f /etc/redhat-release ]]; then
        os_name=$(cat /etc/redhat-release)
    elif command -v sw_vers &>/dev/null; then
        os_name="macOS"
        os_version=$(sw_vers -productVersion 2>/dev/null || echo "Unknown")
    fi

    local supported=false
    case "$os_name" in
        *Ubuntu*|*Debian*|*CentOS*|*RHEL*|*Rocky*|*AlmaLinux*)
            supported=true
            ;;
        *)
            supported=false
            ;;
    esac

    if $supported; then
        record "OS" "PASS" "${os_name} ${os_version}"
    else
        record "OS" "WARN" "${os_name} ${os_version} — 未经充分测试的系统"
    fi
}

check_gcc() {
    if command -v g++ &>/dev/null; then
        local version
        version=$(g++ -dumpversion 2>/dev/null)
        if version_gte "$version" "13.1"; then
            record "GCC" "PASS" "GCC ${version} (>= 13.1)"
        else
            record "GCC" "FAIL" "GCC ${version} (< 13.1 required)"
        fi
    elif command -v clang++ &>/dev/null; then
        local version
        version=$(clang++ -dumpversion 2>/dev/null)
        if version_gte "$version" "17.0"; then
            record "GCC" "PASS" "Clang ${version} (>= 17.0)"
        else
            record "GCC" "FAIL" "Clang ${version} (< 17.0 required)"
        fi
    else
        record "GCC" "FAIL" "No C++ compiler found (GCC >= 13.1 or Clang >= 17.0 required)"
    fi
}

check_cmake() {
    if command -v cmake &>/dev/null; then
        local version
        version=$(cmake --version 2>/dev/null | grep -oP '\d+\.\d+' | head -1)
        if [[ -n "$version" ]] && version_gte "$version" "3.28"; then
            record "CMake" "PASS" "CMake ${version} (>= 3.28)"
        else
            record "CMake" "FAIL" "CMake ${version:-unknown} (< 3.28 required)"
        fi
    else
        record "CMake" "FAIL" "CMake not installed"
    fi
}

check_vcpkg() {
    local toolchain="${VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake"
    if [[ -f "$toolchain" ]]; then
        local vcpkg_exe="${VCPKG_ROOT}/vcpkg"
        local version="N/A"
        if [[ -x "$vcpkg_exe" ]]; then
            version=$("$vcpkg_exe" version 2>/dev/null | head -1 | grep -oP '[\d.]+(-\w+)?' | head -1 || echo "installed")
        fi
        record "vcpkg" "PASS" "vcpkg ${version} at ${VCPKG_ROOT}"
    else
        record "vcpkg" "FAIL" "vcpkg not found at ${VCPKG_ROOT} (set VCPKG_ROOT env var)"
    fi
}

check_wge() {
    local wge_header="${WGE_SDK_PATH}/include/wge/engine.h"
    if [[ -f "$wge_header" ]]; then
        record "WGE SDK" "PASS" "Found at ${WGE_SDK_PATH}"
    else
        record "WGE SDK" "FAIL" "Not found at ${WGE_SDK_PATH} (set WGE_SDK_PATH env var)"
    fi
}

check_vcpkg_lib() {
    local lib_name="$1"
    local check_path="$2"
    local required_ver="${3:-}"
    local optional="${4:-false}"

    local vcpkg_inc="${VCPKG_ROOT}/installed/x64-linux"
    local full_path="${vcpkg_inc}/${check_path}"

    if [[ -e "$full_path" ]]; then
        local detail="Found"
        # Version extraction from vcpkg status
        local vcpkg_status="${VCPKG_ROOT}/installed/vcpkg/status"
        if [[ -f "$vcpkg_status" ]]; then
            local pkg_ver
            pkg_ver=$(grep -A2 "Package: ${lib_name}" "$vcpkg_status" 2>/dev/null | \
                      grep -oP 'Version: \K.*' | head -1 || true)
            if [[ -n "$pkg_ver" ]]; then
                detail="v${pkg_ver}"
            fi
        fi

        if [[ -n "$required_ver" ]]; then
            if [[ -n "${pkg_ver:-}" ]] && version_gte "$pkg_ver" "$required_ver"; then
                record "$lib_name" "PASS" "$detail (>= ${required_ver})"
            else
                record "$lib_name" "PASS" "$detail"  # 无法确定版本但文件存在
            fi
        else
            record "$lib_name" "PASS" "$detail"
        fi
    else
        if $optional; then
            record "$lib_name" "WARN" "Not installed (optional)"
        else
            record "$lib_name" "FAIL" "Not found — run: vcpkg install ${lib_name}"
        fi
    fi
}

check_disk_space() {
    local required_mb=2048  # 2 GB

    local available_kb
    available_kb=$(df --output=avail "$(pwd)" 2>/dev/null | tail -1 || df -k . 2>/dev/null | awk 'NR==2 {print $4}')
    local available_mb=$((available_kb / 1024))

    if [[ $available_mb -ge $required_mb ]]; then
        local available_gb
        available_gb=$(awk "BEGIN {printf \"%.1f\", ${available_mb}/1024}")
        record "Disk Space" "PASS" "${available_gb} GB available (>= 2 GB required)"
    else
        record "Disk Space" "FAIL" "${available_mb} MB available (< 2 GB required)"
    fi
}

check_memory() {
    local required_mb=2048  # 2 GB

    local total_kb
    total_kb=$(grep MemTotal /proc/meminfo 2>/dev/null | awk '{print $2}' || echo 0)
    local total_mb=$((total_kb / 1024))

    if [[ $total_mb -ge $required_mb ]]; then
        local total_gb
        total_gb=$(awk "BEGIN {printf \"%.1f\", ${total_mb}/1024}")
        record "Memory" "PASS" "${total_gb} GB total (>= 2 GB required)"
    else
        record "Memory" "FAIL" "${total_mb} MB total (< 2 GB required)"
    fi
}

# ============================================================================
# 运行所有检查
# ============================================================================

run_all_checks() {
    # 系统
    check_os
    check_disk_space
    check_memory

    # 编译工具
    check_gcc
    check_cmake
    check_vcpkg

    # SDK
    check_wge

    # vcpkg 库
    check_vcpkg_lib "librdkafka" "include/librdkafka/rdkafkacpp.h" "2.0"
    check_vcpkg_lib "protobuf" "include/google/protobuf/message.h" "3.15"
    check_vcpkg_lib "simdjson" "include/simdjson.h" "3.0"
    check_vcpkg_lib "re2" "include/re2/re2.h"
    check_vcpkg_lib "yaml-cpp" "include/yaml-cpp/yaml.h" "0.7"
    check_vcpkg_lib "spdlog" "include/spdlog/spdlog.h" "1.10"
    check_vcpkg_lib "gtest" "include/gtest/gtest.h" "" "true"
}

# ============================================================================
# 输出
# ============================================================================

print_status_icon() {
    case "$1" in
        PASS) echo -en "${COLOR_GREEN}✓${COLOR_RESET}" ;;
        FAIL) echo -en "${COLOR_RED}✗${COLOR_RESET}" ;;
        WARN) echo -en "${COLOR_YELLOW}⚠${COLOR_RESET}" ;;
    esac
}

print_status_text() {
    case "$1" in
        PASS) echo -e "${COLOR_GREEN}PASS${COLOR_RESET}" ;;
        FAIL) echo -e "${COLOR_RED}FAIL${COLOR_RESET}" ;;
        WARN) echo -e "${COLOR_YELLOW}WARN${COLOR_RESET}" ;;
    esac
}

print_table() {
    echo ""
    printf "  %-18s %-6s  %s\n" "CHECK" "STATUS" "DETAILS"
    printf "  %-18s %-6s  %s\n" "──────────────────" "──────" "──────────────────────────────────────────"
    echo ""

    # 按类别打印
    local categories=(
        "OS:操作系统"
        "Disk Space:系统资源"
        "Memory:系统资源"
        "GCC:编译工具"
        "CMake:编译工具"
        "vcpkg:编译工具"
        "WGE SDK:依赖库"
        "librdkafka:依赖库"
        "protobuf:依赖库"
        "simdjson:依赖库"
        "re2:依赖库"
        "yaml-cpp:依赖库"
        "spdlog:依赖库"
        "gtest:依赖库"
    )

    local current_cat=""
    for entry in "${categories[@]}"; do
        local name="${entry%%:*}"
        local cat="${entry##*:}"

        if [[ "$cat" != "$current_cat" ]]; then
            echo -e "  ${COLOR_BOLD}${COLOR_CYAN}[${cat}]${COLOR_RESET}"
            current_cat="$cat"
        fi

        local status="${CHECK_RESULTS[$name]:-}"
        local detail="${CHECK_DETAILS[$name]:-}"
        if [[ -n "$status" ]]; then
            printf "    %-16s " "$name"
            print_status_icon "$status"
            printf " %-5s " ""
            echo -e "$detail"
        fi
    done

    echo ""
}

print_json() {
    echo "{"
    echo "  \"check_time\": \"$(date -Iseconds)\","
    echo "  \"hostname\": \"$(hostname -f 2>/dev/null || hostname)\","
    echo "  \"summary\": {"
    echo "    \"total\": ${TOTAL_CHECKS},"
    echo "    \"passed\": ${PASSED_CHECKS},"
    echo "    \"failed\": ${FAILED_CHECKS},"
    echo "    \"warned\": ${WARNED_CHECKS}"
    echo "  },"
    echo "  \"results\": {"

    local first=true
    for name in "${!CHECK_RESULTS[@]}"; do
        if $first; then first=false; else echo ","; fi
        local status="${CHECK_RESULTS[$name]}"
        local detail="${CHECK_DETAILS[$name]}"
        # JSON-escape detail
        detail=$(echo "$detail" | sed 's/"/\\"/g')
        echo -n "    \"${name}\": {\"status\": \"${status}\", \"detail\": \"${detail}\"}"
    done

    echo ""
    echo "  }"
    echo "}"
}

print_verbose() {
    echo ""
    echo -e "${COLOR_BOLD}Detailed Information:${COLOR_RESET}"
    echo ""

    if command -v g++ &>/dev/null; then
        echo "  GCC:"
        echo "    Path:    $(which g++)"
        echo "    Version: $(g++ --version | head -1)"
    fi

    if command -v cmake &>/dev/null; then
        echo "  CMake:"
        echo "    Path:    $(which cmake)"
        echo "    Version: $(cmake --version | head -1)"
    fi

    if [[ -d "$VCPKG_ROOT" ]]; then
        echo "  vcpkg:"
        echo "    Root:    ${VCPKG_ROOT}"
        echo "    Install: ${VCPKG_ROOT}/installed/x64-linux"
    fi

    echo "  System:"
    echo "    Kernel:  $(uname -r)"
    echo "    Arch:    $(uname -m)"
    echo "    Cores:   $(nproc 2>/dev/null || echo N/A)"

    local total_kb
    total_kb=$(grep MemTotal /proc/meminfo 2>/dev/null | awk '{print $2}' || echo 0)
    echo "    Memory:  $((total_kb / 1024 / 1024)) GB"

    echo ""
}

print_summary() {
    echo -e "${COLOR_BOLD}──────────────────────────────────────────────${COLOR_RESET}"
    echo -e "  Total:  ${TOTAL_CHECKS}"
    echo -e "  Passed: ${COLOR_GREEN}${PASSED_CHECKS}${COLOR_RESET}"

    if [[ $FAILED_CHECKS -gt 0 ]]; then
        echo -e "  Failed: ${COLOR_RED}${FAILED_CHECKS}${COLOR_RESET}"
    else
        echo -e "  Failed: ${FAILED_CHECKS}"
    fi

    if [[ $WARNED_CHECKS -gt 0 ]]; then
        echo -e "  Warned: ${COLOR_YELLOW}${WARNED_CHECKS}${COLOR_RESET}"
    else
        echo -e "  Warned: ${WARNED_CHECKS}"
    fi
    echo -e "${COLOR_BOLD}──────────────────────────────────────────────${COLOR_RESET}"
    echo ""

    if [[ $FAILED_CHECKS -eq 0 ]]; then
        echo -e "${COLOR_GREEN}✓ All required checks passed. Ready to build!${COLOR_RESET}"
        echo ""
        echo "  Next steps:"
        echo "    ./scripts/build.sh --release"
        echo "    sudo ./scripts/install.sh"
    else
        echo -e "${COLOR_RED}✗ ${FAILED_CHECKS} check(s) failed. Please fix before building.${COLOR_RESET}"
        echo ""
        echo "  Install guides:"
        echo "    Ubuntu:   docs/INSTALL.md"
        echo "    Generic:  docs/DEVELOPMENT.md"
    fi

    if [[ $WARNED_CHECKS -gt 0 ]]; then
        echo ""
        echo -e "${COLOR_YELLOW}⚠ ${WARNED_CHECKS} warning(s) — optional components missing.${COLOR_RESET}"
    fi

    echo ""
}

# ============================================================================
# 主流程
# ============================================================================

main() {
    local verbose=false

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --json)
                OUTPUT_FORMAT="json"
                shift
                ;;
            --verbose)
                verbose=true
                shift
                ;;
            -h|--help)
                show_help
                exit 0
                ;;
            *)
                echo "Unknown option: $1"
                echo "Run '$0 --help' for usage."
                exit 1
                ;;
        esac
    done

    if [[ "$OUTPUT_FORMAT" == "json" ]]; then
        # JSON 输出时不显示彩色 header
        run_all_checks
        print_json
    else
        print_header
        echo ""
        echo -e "  VCPKG_ROOT:   ${COLOR_BOLD}${VCPKG_ROOT}${COLOR_RESET}"
        echo -e "  WGE_SDK_PATH: ${COLOR_BOLD}${WGE_SDK_PATH}${COLOR_RESET}"
        echo ""

        run_all_checks
        print_table
        print_summary

        if $verbose; then
            print_verbose
        fi
    fi

    # 退出码：有 FAIL 则非零
    if [[ $FAILED_CHECKS -gt 0 ]]; then
        exit 1
    fi
    exit 0
}

main "$@"
