FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive
ENV CARGO_HOME=/usr/local/cargo
ENV RUSTUP_HOME=/usr/local/rustup
ENV PATH="/usr/local/cargo/bin:${PATH}"

# Tauri v2 Linuxビルド依存パッケージ & Windowsクロスビルドツール (clang, lld, llvm, nsis)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    wget \
    git \
    file \
    build-essential \
    pkg-config \
    libwebkit2gtk-4.1-dev \
    libgtk-3-dev \
    libayatana-appindicator3-dev \
    librsvg2-dev \
    libssl-dev \
    libxdo-dev \
    patchelf \
    clang \
    lld \
    llvm \
    nsis \
    xdg-utils \
    && rm -rf /var/lib/apt/lists/*

# Node.js 22 LTS の導入
RUN curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && rm -rf /var/lib/apt/lists/*

# Rust (stable) & Windows MSVC ターゲット & cargo-xwin の導入
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable --profile minimal \
    && rustup target add x86_64-pc-windows-msvc \
    && cargo install --locked cargo-xwin

WORKDIR /workspace

