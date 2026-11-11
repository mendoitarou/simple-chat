# SimpleChat P2P

**Tauri v2 + React (TypeScript) + Rust (`iroh`)** で構築された、**DesktopMate風透過オーバーレイUI** と **ハイブリッド鍵交換・ユーザ管理サーバ** を備えた1対1 P2Pデスクトップチャットアプリケーションです。

## 主な特徴

1. **DesktopMate風の透過オーバーレイ ＋ 通常履歴ウィンドウの2画面構成**
   - **オーバーレイウィンドウ (`overlay`)**: 背景透過・枠なし・常時最前面（Always on Top）でデスクトップ上にマスコットが常駐。ドラッグで自由な位置へ移動でき、送受信したチャットは吹き出しとしてポップアップして **約5秒後にフェードアウトして消えます**。マスコット下のミニ入力バーからそのまま即返信も可能です。
   - **履歴＆接続設定ウィンドウ (`main`)**: オーバーレイ上で消えたメッセージも含め、セッション中の全チャット履歴を閲覧可能。×ボタンで閉じても裏で維持され、オーバーレイの「📜 履歴・接続」ボタンからいつでも再表示できます。
2. **ハイブリッドP2P通信（管理サーバ ＋ QUIC/Ed25519 E2E暗号化直接通信）**
   - **管理・鍵交換サーバ (`directory-server`)**: ユーザID（`@alice` 等）と Ed25519 公開鍵（`NodeId`）・最新接続チケットを **Ed25519電子署名検証付き** で登録・管理し、なりすまし登録や公開鍵すり替え（MITM）を防止します。
   - **P2P直接チャット**: オンラインユーザ一覧からワンクリックで接続でき、**チャット本文は管理サーバを一切経由せず Iroh (QUIC + TLS 1.3) でピア間を直接暗号化通信**します。
   - **手動チケット接続も併用可能**: 管理サーバを使わない環境でも、接続チケット文字列のコピー＆ペーストだけでP2P接続できます。
3. **Docker完結のLinux / Windows両対応ビルド**
   - ホスト環境を汚さず、Dockerコンテナ上で自動テスト・Linux向けビルド・Windows向けクロスビルド（`cargo-xwin` + `NSIS`）を実行できます。

---

## 使い方（Dockerによるテスト・ビルド・起動）

### 1. 自動テストの実行
フロントエンドのビルド、管理サーバのEd25519署名検証テスト、およびRustのIroh P2P 2ノード間通信テストを実行します：
```bash
docker compose run --rm test
```

### 2. 管理サーバ（Directory Server）の起動
ユーザ登録・公開鍵自動交換・オンライン一覧を使う場合は、バックグラウンドで管理サーバ（ポート `3100`）を起動します：
```bash
docker compose up -d directory-server
```

### 3. Linux向けビルドと起動
```bash
docker compose run --rm build-linux
```
ビルド完了後、以下のファイルが `./artifacts/linux/` に出力されます：
- `artifacts/linux/simple-chat` （Linux実行バイナリ）
- `artifacts/linux/*.deb` （Debian/Ubuntuパッケージ）

起動するには：
```bash
./scripts/run-linux.sh
```
（同一PC上で2つ同時に起動して1対1のP2P通信テストを行うことも可能です）

### 4. Windows向けクロスビルド（`.exe` / NSISインストーラー）
Linux上のDockerから Windows (`x86_64-pc-windows-msvc`) 向けの実行ファイルとインストーラーを生成します：
```bash
docker compose run --rm build-windows
```
ビルド完了後、以下のファイルが `./artifacts/windows/` に出力されます：
- `artifacts/windows/simple-chat.exe` （Windowsポータブル実行ファイル）
- `artifacts/windows/*-setup.exe` （Windows NSISインストーラー）
