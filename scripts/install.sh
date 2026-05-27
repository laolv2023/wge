#!/usr/bin/env bash
# =============================================================================
# WGE-Kafka Detector — 安装部署脚本
#
# 用途:
#   将编译好的 wge-detector 安装部署到系统：复制二进制文件、配置文件、
#   创建 systemd 服务、配置目录结构和日志目录。
#
# 用法:
#   sudo ./scripts/install.sh              # 安装部署
#   sudo ./scripts/install.sh --uninstall  # 卸载
#   sudo ./scripts/install.sh --no-start   # 安装但不启动服务
#
# 选项:
#   --uninstall       卸载 WGE-Kafka Detector
#   --no-start        安装后不启动服务（默认会 enable + start）
#   -h, --help        显示帮助
#
# 目录结构:
#   /etc/wge/                  配置文件目录
#   /var/log/wge/              日志目录
#   /var/lib/wge/wal/          WAL 预写日志目录
#   /usr/local/bin/            二进制文件目录
#   /etc/systemd/system/       systemd 服务文件
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
readonly PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly BUILD_DIR="${BUILD_DIR:-${PROJECT_ROOT}/build}"

readonly CONFIG_DIR="/etc/wge"
readonly LOG_DIR="/var/log/wge"
readonly WAL_DIR="/var/lib/wge/wal"
readonly BIN_DIR="/usr/local/bin"
readonly SYSTEMD_DIR="/etc/systemd/system"

readonly BINARY_NAME="wge-detector"
readonly CONFIG_FILE="wge-detector.yaml"
readonly LOG_MAPPING_FILE="log_mapping.yaml"
readonly SERVICE_NAME="wge-detector.service"

readonly SERVICE_USER="wge"
readonly SERVICE_GROUP="wge"

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
    echo -e "${COLOR_BOLD}${COLOR_CYAN}  WGE-Kafka Detector — Install / Deploy Script${COLOR_RESET}"
    echo -e "${COLOR_BOLD}${COLOR_CYAN}══════════════════════════════════════════════════════════════${COLOR_RESET}"
    echo ""
}

show_help() {
    cat <<EOF
Usage: $0 [OPTIONS]

Options:
  --uninstall       卸载 WGE-Kafka Detector
  --no-start        安装后不启动服务
  -h, --help        显示帮助

Installation directories:
  Config:    ${CONFIG_DIR}/
  Logs:      ${LOG_DIR}/
  WAL:       ${WAL_DIR}/
  Binary:    ${BIN_DIR}/${BINARY_NAME}
  Service:   ${SYSTEMD_DIR}/${SERVICE_NAME}

Examples:
  sudo $0                     # 完整安装部署
  sudo $0 --no-start          # 安装但不启动
  sudo $0 --uninstall         # 卸载
EOF
}

check_root() {
    if [[ $EUID -ne 0 ]]; then
        print_err "This script requires root privileges."
        echo ""
        echo "  Please run with sudo:"
        echo "    sudo $0 $*"
        echo ""
        exit 1
    fi
}

# ============================================================================
# 卸载
# ============================================================================

do_uninstall() {
    print_header
    print_step "卸载 WGE-Kafka Detector..."

    # 停止并禁用服务
    if systemctl is-active --quiet "$SERVICE_NAME" 2>/dev/null; then
        print_step "停止服务..."
        systemctl stop "$SERVICE_NAME"
        print_ok "服务已停止"
    fi

    if systemctl is-enabled --quiet "$SERVICE_NAME" 2>/dev/null; then
        print_step "禁用服务..."
        systemctl disable "$SERVICE_NAME"
        print_ok "服务已禁用"
    fi

    # 删除 systemd 文件
    if [[ -f "${SYSTEMD_DIR}/${SERVICE_NAME}" ]]; then
        rm -f "${SYSTEMD_DIR}/${SERVICE_NAME}"
        systemctl daemon-reload
        print_ok "已删除: ${SYSTEMD_DIR}/${SERVICE_NAME}"
    fi

    # 删除二进制文件
    if [[ -f "${BIN_DIR}/${BINARY_NAME}" ]]; then
        rm -f "${BIN_DIR}/${BINARY_NAME}"
        print_ok "已删除: ${BIN_DIR}/${BINARY_NAME}"
    fi

    # 删除配置文件（保留日志和 WAL 数据）
    if [[ -f "${CONFIG_DIR}/${CONFIG_FILE}" ]]; then
        rm -f "${CONFIG_DIR}/${CONFIG_FILE}"
        print_ok "已删除: ${CONFIG_DIR}/${CONFIG_FILE}"
    fi

    if [[ -f "${CONFIG_DIR}/${LOG_MAPPING_FILE}" ]]; then
        rm -f "${CONFIG_DIR}/${LOG_MAPPING_FILE}"
        print_ok "已删除: ${CONFIG_DIR}/${LOG_MAPPING_FILE}"
    fi

    # 清理空配置目录
    if [[ -d "$CONFIG_DIR" ]]; then
        rmdir "$CONFIG_DIR" 2>/dev/null && print_ok "已删除空目录: ${CONFIG_DIR}" || true
    fi

    echo ""
    print_warn "以下目录未被删除（包含运行时数据）："
    print_warn "  日志目录: ${LOG_DIR}"
    print_warn "  WAL 目录: ${WAL_DIR}"
    print_warn "如需完全清除，请手动执行: rm -rf ${LOG_DIR} ${WAL_DIR}"

    echo ""
    print_ok "卸载完成"
    echo ""
}

# ============================================================================
# 安装
# ============================================================================

create_user() {
    if id "$SERVICE_USER" &>/dev/null; then
        print_ok "用户 ${SERVICE_USER} 已存在"
    else
        print_step "创建服务用户 ${SERVICE_USER}..."
        useradd --system --no-create-home --shell /usr/sbin/nologin \
            --home-dir /var/lib/wge "$SERVICE_USER" 2>/dev/null || {
            # 备选方案：兼容旧版 useradd
            useradd -r -s /usr/sbin/nologin -d /var/lib/wge "$SERVICE_USER"
        }
        print_ok "用户 ${SERVICE_USER} 已创建"
    fi

    if getent group "$SERVICE_GROUP" &>/dev/null; then
        print_ok "用户组 ${SERVICE_GROUP} 已存在"
    else
        groupadd --system "$SERVICE_GROUP" 2>/dev/null || groupadd -r "$SERVICE_GROUP"
        usermod -a -G "$SERVICE_GROUP" "$SERVICE_USER"
        print_ok "用户组 ${SERVICE_GROUP} 已创建"
    fi
}

create_directories() {
    print_step "创建目录..."

    local dirs=("$CONFIG_DIR" "$LOG_DIR" "$WAL_DIR" "$BIN_DIR")
    for dir in "${dirs[@]}"; do
        if [[ ! -d "$dir" ]]; then
            mkdir -p "$dir"
            print_ok "已创建: ${dir}"
        else
            print_ok "已存在: ${dir}"
        fi
    done

    # 设置所有权
    chown -R "${SERVICE_USER}:${SERVICE_GROUP}" "$CONFIG_DIR"
    chown -R "${SERVICE_USER}:${SERVICE_GROUP}" "$LOG_DIR"
    chown -R "${SERVICE_USER}:${SERVICE_GROUP}" "$WAL_DIR"
    chmod 750 "$CONFIG_DIR"
    chmod 750 "$LOG_DIR"
    chmod 750 "$WAL_DIR"

    print_ok "目录权限已设置"
}

copy_binary() {
    print_step "复制二进制文件..."

    local src="${BUILD_DIR}/${BINARY_NAME}"
    local dst="${BIN_DIR}/${BINARY_NAME}"

    if [[ ! -f "$src" ]]; then
        # 也检查 CMake 安装路径
        src="${INSTALL_PREFIX:-/usr/local}/bin/${BINARY_NAME}"
        if [[ ! -f "$src" ]]; then
            print_err "二进制文件未找到: ${BUILD_DIR}/${BINARY_NAME}"
            print_err "请先运行: ./scripts/build.sh --install"
            print_err "或指定 BUILD_DIR 环境变量指向编译输出目录"
            exit 1
        fi
    fi

    cp "$src" "$dst"
    chmod 755 "$dst"
    chown "root:root" "$dst"
    print_ok "已复制: ${dst}"

    # 显示版本信息
    if "$dst" --version &>/dev/null; then
        echo ""
        "$dst" --version 2>&1 | while IFS= read -r line; do
            echo "           $line"
        done
    fi
}

copy_configs() {
    print_step "复制配置文件..."

    # 主配置文件
    local src_config="${PROJECT_ROOT}/config/${CONFIG_FILE}"
    local dst_config="${CONFIG_DIR}/${CONFIG_FILE}"

    if [[ -f "$src_config" ]]; then
        if [[ -f "$dst_config" ]]; then
            print_warn "配置文件已存在，跳过: ${dst_config}"
            print_warn "  如需覆盖，请手动: cp ${src_config} ${dst_config}"
        else
            cp "$src_config" "$dst_config"
            chmod 640 "$dst_config"
            chown "${SERVICE_USER}:${SERVICE_GROUP}" "$dst_config"
            print_ok "已复制: ${dst_config}"
        fi
    else
        print_warn "默认配置文件未找到: ${src_config}"
        print_warn "  请手动创建: ${dst_config}"
        print_warn "  参考: docs/INSTALL.md"
    fi

    # 日志映射配置
    local src_logmap="${PROJECT_ROOT}/config/${LOG_MAPPING_FILE}"
    local dst_logmap="${CONFIG_DIR}/${LOG_MAPPING_FILE}"

    if [[ -f "$src_logmap" ]]; then
        if [[ -f "$dst_logmap" ]]; then
            print_warn "日志映射配置已存在，跳过: ${dst_logmap}"
        else
            cp "$src_logmap" "$dst_logmap"
            chmod 640 "$dst_logmap"
            chown "${SERVICE_USER}:${SERVICE_GROUP}" "$dst_logmap"
            print_ok "已复制: ${dst_logmap}"
        fi
    else
        print_warn "日志映射配置未找到: ${src_logmap}"
        print_warn "  请手动创建: ${dst_logmap}"
    fi
}

install_systemd() {
    print_step "安装 systemd 服务..."

    local service_file="${SYSTEMD_DIR}/${SERVICE_NAME}"

    if [[ -f "$service_file" ]]; then
        print_warn "服务文件已存在，将覆盖: ${service_file}"
    fi

    cat > "$service_file" <<'SERVICEEOF'
[Unit]
Description=WGE-Kafka Detector Service
After=network.target kafka.service
Documentation=https://github.com/laolv2023/wge

[Service]
Type=simple
User=wge
Group=wge
ExecStart=/usr/local/bin/wge-detector --config /etc/wge/wge-detector.yaml
ExecReload=/bin/kill -HUP $MAINPID
Restart=on-failure
RestartSec=10
LimitNOFILE=65536
StandardOutput=journal
StandardError=journal
SyslogIdentifier=wge-detector

# 安全加固
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
ReadWritePaths=/var/log/wge /var/lib/wge
ReadOnlyPaths=/etc/wge

[Install]
WantedBy=multi-user.target
SERVICEEOF

    chmod 644 "$service_file"
    print_ok "已创建: ${service_file}"

    systemctl daemon-reload
    print_ok "systemd daemon-reload 完成"
}

enable_and_start() {
    print_step "启用并启动服务..."

    if systemctl is-enabled --quiet "$SERVICE_NAME" 2>/dev/null; then
        print_ok "服务已启用"
    else
        systemctl enable "$SERVICE_NAME"
        print_ok "服务已启用 (开机自启)"
    fi

    if systemctl is-active --quiet "$SERVICE_NAME" 2>/dev/null; then
        print_warn "服务已在运行中，执行重启..."
        systemctl restart "$SERVICE_NAME"
        print_ok "服务已重启"
    else
        systemctl start "$SERVICE_NAME"
        print_ok "服务已启动"
    fi

    # 等待并检查状态
    sleep 2
    if systemctl is-active --quiet "$SERVICE_NAME" 2>/dev/null; then
        print_ok "服务运行正常"
    else
        print_err "服务启动失败，查看日志:"
        echo ""
        echo "  journalctl -u ${SERVICE_NAME} -n 50 --no-pager"
        echo "  systemctl status ${SERVICE_NAME}"
        echo ""
        # 打印最近日志帮助诊断
        echo -e "${COLOR_YELLOW}--- 最近日志 ---${COLOR_RESET}"
        journalctl -u "$SERVICE_NAME" -n 20 --no-pager 2>/dev/null || true
        exit 1
    fi
}

print_post_install() {
    echo ""
    echo -e "${COLOR_GREEN}══════════════════════════════════════════════════════════════${COLOR_RESET}"
    echo -e "${COLOR_GREEN}  安装部署完成!${COLOR_RESET}"
    echo -e "${COLOR_GREEN}══════════════════════════════════════════════════════════════${COLOR_RESET}"
    echo ""
    echo -e "  ${COLOR_BOLD}配置文件${COLOR_RESET}:     ${CONFIG_DIR}/${CONFIG_FILE}"
    echo -e "  ${COLOR_BOLD}日志映射${COLOR_RESET}:     ${CONFIG_DIR}/${LOG_MAPPING_FILE}"
    echo -e "  ${COLOR_BOLD}二进制文件${COLOR_RESET}:   ${BIN_DIR}/${BINARY_NAME}"
    echo -e "  ${COLOR_BOLD}日志目录${COLOR_RESET}:     ${LOG_DIR}/"
    echo -e "  ${COLOR_BOLD}WAL 目录${COLOR_RESET}:     ${WAL_DIR}/"
    echo -e "  ${COLOR_BOLD}systemd 服务${COLOR_RESET}: ${SYSTEMD_DIR}/${SERVICE_NAME}"
    echo ""
    echo -e "  ${COLOR_BOLD}常用命令:${COLOR_RESET}"
    echo "    systemctl status ${SERVICE_NAME}    # 查看服务状态"
    echo "    systemctl restart ${SERVICE_NAME}   # 重启服务"
    echo "    journalctl -u ${SERVICE_NAME} -f    # 查看日志"
    echo "    ${BINARY_NAME} --version            # 查看版本"
    echo ""
}

# ============================================================================
# 主流程
# ============================================================================

main() {
    local DO_START=true

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --uninstall)
                check_root
                do_uninstall
                exit 0
                ;;
            --no-start)
                DO_START=false
                shift
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

    check_root
    print_header

    print_step "开始安装部署..."

    create_user
    create_directories
    copy_binary
    copy_configs
    install_systemd

    if $DO_START; then
        enable_and_start
    else
        print_warn "跳过服务启动 (--no-start)"
        systemctl daemon-reload
        systemctl enable "$SERVICE_NAME" 2>/dev/null || true
        print_ok "服务已启用但未启动。手动启动: systemctl start ${SERVICE_NAME}"
    fi

    print_post_install
}

main "$@"
