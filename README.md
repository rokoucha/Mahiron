# Mahiron 5

<img width="1655" height="1350" alt="Dashboard example" src="https://github.com/user-attachments/assets/19e84d6b-b257-4626-9917-386719cf5365" />

Yet another DVR Tuner Server for Japanese TV.

## 特徴

- Mirakurun互換のAPI
- Go言語で書かれていてシングルバイナリで動作
- 分かりやすいダッシュボード
- リアルタイム番組表更新
- ケーブルテレビの再送信など複数経路のTSを統合
- OpenTelemetryによる高い可観測性

## セットアップ

[リリース](https://github.com/rokoucha/Mahiron/releases)より最新のバージョンをダウンロードするか、[コンテナイメージ](https://github.com/rokoucha/Mahiron/pkgs/container/mahiron)をpullしてください。

mainブランチの最新ビルドを試す場合は、[CIの実行履歴](https://github.com/rokoucha/Mahiron/actions/workflows/ci.yml)からバイナリのartifactをダウンロードできます。コンテナイメージは `ghcr.io/rokoucha/mahiron:nightly` でpullできます。日時指定の `YYYY.MMDD.HHMMSS-nightly` や、特定のコミットを直接指定する `sha-<短縮コミットSHA>` タグもあります。

設定は[サンプル](https://github.com/rokoucha/Mahiron/tree/main/config)を参考に、実行ファイルと同じディレクトリの `config` フォルダ内に配置してください。別なフォルダにする場合は、実行時に `-config-dir <path>` オプションを指定してください。

起動すると、自動的に放送サービスをスキャンし、EPGやロゴを取得します。
実行状態はダッシュボードで確認してください。

## Mirakurunとの差分

代表的な差分です。これ以外にも非互換な部分があります。

### API

- 設定系のAPIは対応していません
  - `/config` 以下の全て
  - `PUT /restart`
- ChannelTypeは好きな文字列を指定できます
- Serviceに以下のフィールドを追加しています
  - transportStreamId
  - eitScheduleFlag
  - eitPresentFollowing
  - epgLastAttemptAt
  - epgLastError
- Channelに以下のフィールドを追加しています
  - routes
- TunerDeviceに以下のフィールドを追加しています
  - currentChannel*
  - tunedChannel*
- `/api/version` にserverフィールドを追加しています
  - 値は常に `mahiron` です
- JobItemに以下のフィールドを追加しています
  - nextRunAt
  - result
- RelatedItemに以下のフィールドを追加しています
  - transportStreamId

### 設定

- チャンネル設定(`channels.yml`)は互換です
  - ケーブルテレビの再送信などで複数経路で同じTSを受信できる場合の設定フィールド(`routes`)を追加しています
- チューナー設定(`tuners.yml`)はリモートMirakurunを除いて互換です
- サーバー設定(`server.yml`)は非互換です
- リモートMirakurun機能はありませんが、Mirakurun互換サーバーに接続する機能があります(`remotes.yml`)

## ライセンス

Copyright (c) 2026 Rokoucha

Licensed under the Apache License, Version 2.0.

Mahiron's logo is licensed under a Creative Commons Attribution-ShareAlike 4.0 International License.
