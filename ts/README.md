# ts - MPEG-2 TS ネイティブ処理パッケージ

本パッケージは外部コマンドに依存せず、Go ネイティブで MPEG-2 Transport Stream を処理する。

## 設計方針

- 188 バイト固定パケットのみをサポート（ARIB 運用に 192/204 バイトは不要）
- 壊れた TS に対して寛容に動作する
  - sync byte 喪失時は次の 188 バイト境界を探して復帰
  - transport_error_indicator 付きパケットは無視
  - CRC 不一致の section は破棄
  - 未知の descriptor はスキップ
- EPG 出力は `internal/epg` の EIT JSON 表現と互換にし、`program` は TS/EIT 詳細を知らない状態を保つ

## ディレクトリ構成

```
ts/
├── README.md              # 本ファイル
├── packet.go              # TS パケット読み込み・sync 復帰
├── section.go             # section 再構成・CRC 検証
├── psi.go                 # PSI 共通ユーティリティ
├── pat.go                 # PAT パーサ
├── pmt.go                 # PMT パーサ
├── sdt.go                 # SDT パーサ
├── eit.go                 # EIT section パーサ
├── descriptors.go         # 記述子共通・DVB 共通記述子
├── descriptor_service.go  # Service 記述子（ARIB）
├── descriptor_short_event.go      # ShortEvent 記述子
├── descriptor_extended_event.go   # ExtendedEvent 記述子
├── descriptor_content.go          # Content 記述子
├── descriptor_component.go        # Component 記述子
├── descriptor_audio_component.go  # AudioComponent 記述子（ARIB）
├── descriptor_event_group.go      # EventGroup 記述子（ARIB）
├── descriptor_series.go           # Series 記述子（ARIB）
├── aribstr.go             # ARIB STD-B24 文字列 → UTF-8 変換 API
├── aribstr/               # ARIB STD-B24 文字列デコーダ実装と変換表
├── filter.go              # サービスフィルタ
├── scanner.go             # サービススキャナ
└── eitcollector.go        # EITPF / EITS 収集
```

## 実装済みの機能

### 基盤

- `packet.go`: 188 バイトパケットの読み込み、PID 抽出、adaptation field 判定、sync 喪失時の復帰
- `section.go`: 同一 PID 上の section 断片を再構成、CRC32-MPEG-2 検証、continuity counter の不連続検知
- `psi.go`: table_id / section_number / version_number / current_next_indicator 等の共通読み取り

### サービスフィルタ

- `pat.go`, `pmt.go`: PAT/PMT パース
- `filter.go`: 対象 service_id の PMT と関連 PID（PCR/映像/音声/字幕）を抽出し、それ以外を落とす
- `internal/filter` から利用されるサービスフィルタ実装

### サービススキャナ

- `sdt.go`: SDT パース
- `descriptor_service.go`: Service 記述子からサービス名・サービスタイプを取得
- `aribstr.go`, `aribstr/`: サービス名の ARIB STD-B24 文字列変換
- `scanner.go`: PAT/PMT/SDT から `service.scanService` 相当の JSON 配列を出力
- `internal/servicescan` から利用されるサービススキャナ実装

### EITPF 収集

- `eit.go`: EIT section のパース
- `descriptor_short_event.go`, `descriptor_content.go`, `descriptor_component.go`, `descriptor_audio_component.go`
- `eitcollector.go`: `CollectEITPF` を実装

### EITS 収集

- `descriptor_extended_event.go`, `descriptor_event_group.go`, `descriptor_series.go`
- `eitcollector.go`: `CollectEITS` を実装
- `internal/epg` から利用される EITPF / EITS collector 実装

## テスト方針

- ユニットテストはリポジトリにコミット可能な合成 TS fixture を使用
- 実際の地上波/BS 録画データはローカル開発時のみ使用し、リポジトリにはコミットしない
- `ts/testdata/local` の比較 fixture はローカル開発時の回帰確認に使用する
