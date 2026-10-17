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
- データ放送API
- ISDB-S3の実験的サポート

## セットアップ

[リリース](https://github.com/rokoucha/Mahiron/releases)より最新のバージョンをダウンロードするか、[コンテナイメージ](https://github.com/rokoucha/Mahiron/pkgs/container/mahiron)をpullしてください。

mainブランチの最新ビルドを試す場合は、[CIの実行履歴](https://github.com/rokoucha/Mahiron/actions/workflows/ci.yml)からバイナリのartifactをダウンロードできます。コンテナイメージは `ghcr.io/rokoucha/mahiron:nightly` でpullできます。日時指定の `YYYY.MMDD.HHMMSS-nightly` や、特定のコミットを直接指定する `sha-<短縮コミットSHA>` タグもあります。

設定は[サンプル](https://github.com/rokoucha/Mahiron/tree/main/config)を参考に、実行ファイルと同じディレクトリの `config` フォルダ内に配置してください。別なフォルダにする場合は、実行時に `-config-dir <path>` オプションを指定してください。

起動すると、自動的に放送サービスをスキャンし、EPGやロゴを取得します。

EPGの直接受信はONIDとTS/TLVストリームIDの組ごと、リモートの保存済みEPG取得はネットワークごとにジョブを分けます。同じストリームを受信できるチャンネルは受信候補としてまとめ、ストリームIDが未取得の場合は暫定的にチャンネルごとに分けます。同じネットワークの別ストリームの番組表も保存しますが、そのストリームの取得状態は更新せず、詳細取得は別途実行します。各ジョブは未取得のサービスがあるもの、最終取得成功が古いものの順に処理し、全対象サービスが直近30分以内に取得成功していれば定期取得を見送ります。取得は `maxConcurrentJobs` の範囲で並列実行し、サービススキャン中は待機します。

EITの版番号はサブテーブルごとに独立し、actual/other間でも一致しません（ARIB TR-B15 §6.1）。版の変化を受信世代として追跡して遅延保存された旧版を除外し、自TSのactualを取得した番組はotherによる上書きや重複削除から保護します。otherは未取得番組の保存と不足情報の補完に利用します。取得元と版番号は既存の番組JSONへ保存し、受信世代はプロセス内だけで管理します。異なる取得元のother同士も版を比較できないため既存情報を優先し、actualの取得で確定します。同一取得中に退役版が再登場して新旧を判別できない場合は破棄し、次の取得で再評価します。版切替中の旧セクションは不足情報の補完だけに使い、未保存の番組は新版の受信を待ちます。
実行状態はダッシュボードで確認してください。

## Mirakurunとの差分

代表的な差分です。これ以外にも非互換な部分があります。

### API

- 設定の変更などには対応していません
  - APIによる設定の参照・変更
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
- リモートチューナーの `types` は、有効な `routes` に対応するローカルの種類を返します
  - `currentChannelType` / `currentChannel` はローカルの対応先が一意の場合は変換した値を、それ以外の場合はリモートが報告した値を返します
  - `tunedChannelType` / `tunedChannel` はリモートで実際に選局しているチャンネルの種類と番号を返します
- `/api/version` にserverフィールドを追加しています
  - 値は常に `mahiron` です
- JobItemに以下のフィールドを追加しています
  - nextRunAt
  - result
- RelatedItemに以下のフィールドを追加しています
  - transportStreamId
- `/api/services/{id}/data-broadcast/bml` でデータ放送(ARIB STD-B24)用のAPIを実装しています
  - 詳細はAPIドキュメントを参照してください

### 設定

- チャンネル設定(`channels.yml`)は互換です
  - ケーブルテレビの再送信などで複数経路で同じTSを受信できる場合の設定フィールド(`routes`)を追加しています
  - TSMFの設定はローカルのみ反映されます
- チューナー設定(`tuners.yml`)はリモートMirakurunを除いて互換です
- サーバー設定(`server.yml`)は非互換です
- リモートMirakurun機能はありませんが、Mirakurun互換サーバーに接続する機能があります(`remotes.yml`)

## ライセンス

Copyright (c) 2026 Rokoucha

Licensed under the Apache License, Version 2.0.

Mahiron's logo is licensed under a Creative Commons Attribution-ShareAlike 4.0 International License.
