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

### EPGの自動取得

既定は対象ごとの期限を確認する常駐方式です。`epgRefreshInterval`（ミリ秒、既定1800000）を更新目標とし、未取得・期限超過のストリームから取得します。視聴が優先されるため、混雑時の期限内取得は保証されません。

常駐取得では、基本EPGと拡張確認の期限を分けます。拡張の送出が不明な対象は、初回と`epgExtendedProbeInterval`（既定21600000ms＝6時間）ごとに受信上限まで調査します。それ以外は基本EPG完了後、対象の進展が`epgUnknownExtendedIdleTime`（既定30000ms）止まれば部分保存して枠を返します。0を指定すると短縮を無効にできます。この時間は運用上の受信予算で、非送出の判定値ではありません。拡張を観測済み、またはBIT等が送出を明示した対象は短縮しません。

`epgReady`は全モードで基本EPGが揃えば有効になり、`epgUpdatedAt`は引き続き完全取得の成功時刻です。APIの`epgBasicUpdatedAt`・`epgExtendedProbeAt`・`epgExtendedObserved`・`epgPartialAt`で基本更新、長めの確認、拡張観測、部分取得を区別できます。部分取得は`epg_partial`として記録し、基本の更新期限または拡張の確認期限に再取得します。状態はDBに保存します。


`epgGatheringMode: continuous` を設定する場合、`jobs` から `epg-gatherer` のcronを削除してください。明示された旧cronは `scheduled` モードとして従来どおり動作します。`jobs` を指定しEPGを省いた既存設定では自動取得を開始しません。明示的に止める場合は `epgGatheringMode: disabled` を使います。

EPG受信はスキャン・ロゴ取得などと `maxConcurrentJobs` の共通枠を使い、外部の保存済みEPG同期だけを別枠で1件まで実行します。受信できた部分は保存しますが、基本・詳細の対象sectionが揃わない取得は最終成功時刻を更新せず、間隔を広げて再試行します。詳細の非送出はBIT/MH-BITの適用中の運用情報で確認します。地上波のサービス所属は実際のSDTと照合します。番組名欠落などの品質警告は完全性と区別し、sectionが揃っていれば成功時刻を更新します。他ストリームの基本情報だけでは詳細取得を省略しません。`epgRetrievalTime` は1候補の受信時間の上限であり、上限到達を取得完了とは見なしません。

ジョブ画面の `continuous` 行から再評価を要求でき、個々の取得は従来どおり履歴の確認・中止・再実行が可能です。期限切れ番組の削除は常駐処理から1時間ごとに実行します。

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
