#!/usr/bin/env sh
set -eu

# RPC Plane installer
# Usage: curl -sSf https://rpcplane.dev/install.sh | sh
#
# Installs the latest rpc-plane binary to /usr/local/bin (or $INSTALL_DIR).

REPO="rpcplane/rpc-plane"
INSTALL_DIR="${INSTALL_DIR:-/usr/local/bin}"
BINARY="rpc-plane"

# ── Detect OS and architecture ────────────────────────────────────────────────

detect_target() {
    local os arch

    case "$(uname -s)" in
        Linux)  os="unknown-linux-gnu" ;;
        Darwin) os="apple-darwin"      ;;
        *)
            echo "error: unsupported OS: $(uname -s)" >&2
            exit 1
            ;;
    esac

    case "$(uname -m)" in
        x86_64)          arch="x86_64"  ;;
        aarch64 | arm64) arch="aarch64" ;;
        *)
            echo "error: unsupported architecture: $(uname -m)" >&2
            exit 1
            ;;
    esac

    echo "${arch}-${os}"
}

# ── Resolve latest version from GitHub API ───────────────────────────────────

latest_version() {
    if command -v curl >/dev/null 2>&1; then
        curl -sSf "https://api.github.com/repos/${REPO}/releases/latest" \
            | grep '"tag_name"' \
            | sed 's/.*"tag_name": "\(.*\)".*/\1/'
    elif command -v wget >/dev/null 2>&1; then
        wget -qO- "https://api.github.com/repos/${REPO}/releases/latest" \
            | grep '"tag_name"' \
            | sed 's/.*"tag_name": "\(.*\)".*/\1/'
    else
        echo "error: curl or wget is required" >&2
        exit 1
    fi
}

# ── Download ──────────────────────────────────────────────────────────────────

download() {
    local url="$1"
    local dest="$2"

    if command -v curl >/dev/null 2>&1; then
        curl -sSfL "$url" -o "$dest"
    elif command -v wget >/dev/null 2>&1; then
        wget -qO "$dest" "$url"
    else
        echo "error: curl or wget is required" >&2
        exit 1
    fi
}

# ── Main ──────────────────────────────────────────────────────────────────────

main() {
    local target version asset url tmp

    target="$(detect_target)"
    version="${VERSION:-$(latest_version)}"

    if [ -z "$version" ]; then
        echo "error: could not determine latest version" >&2
        exit 1
    fi

    asset="${BINARY}-${target}"
    url="https://github.com/${REPO}/releases/download/${version}/${asset}"
    tmp="$(mktemp)"

    echo "Installing rpc-plane ${version} (${target})..."
    download "$url" "$tmp"

    # Verify checksum if sha256sum / shasum is available.
    if command -v sha256sum >/dev/null 2>&1 || command -v shasum >/dev/null 2>&1; then
        local checksum_url checksum_tmp

        checksum_url="${url}.sha256"
        checksum_tmp="$(mktemp)"

        if download "$checksum_url" "$checksum_tmp" 2>/dev/null; then
            # The .sha256 file contains "HASH  filename". Rewrite the filename to
            # match the temp path so sha256sum -c can verify it.
            sed "s|${asset}|${tmp}|g" "$checksum_tmp" > "${checksum_tmp}.check"
            if command -v sha256sum >/dev/null 2>&1; then
                sha256sum -c "${checksum_tmp}.check" --status
            else
                shasum -a 256 -c "${checksum_tmp}.check" --status
            fi
            rm -f "$checksum_tmp" "${checksum_tmp}.check"
            echo "Checksum verified."
        fi
    fi

    # Verify Sigstore cosign signature if cosign is available. This proves the
    # binary was built and signed by our GitHub Actions release workflow.
    if command -v cosign >/dev/null 2>&1; then
        local bundle_url bundle_tmp identity_re

        bundle_url="${url}.cosign.bundle"
        bundle_tmp="$(mktemp)"
        identity_re="^https://github\\.com/${REPO}/\\.github/workflows/release\\.yml@refs/tags/v[0-9].*$"

        if download "$bundle_url" "$bundle_tmp" 2>/dev/null; then
            if cosign verify-blob \
                    --bundle "$bundle_tmp" \
                    --certificate-identity-regexp "$identity_re" \
                    --certificate-oidc-issuer "https://token.actions.githubusercontent.com" \
                    "$tmp" >/dev/null 2>&1; then
                echo "Cosign signature verified."
            else
                echo "error: cosign signature verification failed for $asset" >&2
                rm -f "$bundle_tmp" "$tmp"
                exit 1
            fi
            rm -f "$bundle_tmp"
        else
            rm -f "$bundle_tmp"
        fi
    else
        echo "Note: install cosign for signature verification — https://docs.sigstore.dev/cosign/system_config/installation/"
    fi

    chmod +x "$tmp"

    # Install — try without sudo first, fall back to sudo.
    if [ -w "$INSTALL_DIR" ]; then
        mv "$tmp" "${INSTALL_DIR}/${BINARY}"
    else
        echo "Requires elevated permissions to write to ${INSTALL_DIR}."
        sudo mv "$tmp" "${INSTALL_DIR}/${BINARY}"
    fi

    echo "Installed to ${INSTALL_DIR}/${BINARY}"
    echo ""
    echo "Get started:"
    echo "  rpc-plane init          # generate a starter config"
    echo "  rpc-plane run           # start the proxy"
    echo "  rpc-plane --help        # full CLI reference"
}

main "$@"
