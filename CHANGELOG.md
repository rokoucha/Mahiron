# Changelog

## [v5.1.0](https://github.com/rokoucha/Mahiron/compare/v5.0.15...v5.1.0) - 2026-08-23

- Update dependency eslint to v10.8.1 by @renovate[bot] in https://github.com/rokoucha/Mahiron/pull/101
- Update module github.com/ogen-go/ogen to v1.24.0 by @renovate[bot] in https://github.com/rokoucha/Mahiron/pull/102
- Update module modernc.org/sqlite to v1.56.0 by @renovate[bot] in https://github.com/rokoucha/Mahiron/pull/105
- Update dependency eslint-plugin-react-refresh to v0.5.4 by @renovate[bot] in https://github.com/rokoucha/Mahiron/pull/106
- Update dependency typescript-eslint to v8.67.0 by @renovate[bot] in https://github.com/rokoucha/Mahiron/pull/107
- Update dependency globals to v17.11.0 by @renovate[bot] in https://github.com/rokoucha/Mahiron/pull/108
- Update module golang.org/x/text to v0.41.0 by @renovate[bot] in https://github.com/rokoucha/Mahiron/pull/109
- Serve a Server header identifying Mahiron by @rokoucha in https://github.com/rokoucha/Mahiron/pull/115
- リモートの可用性チェックのタイムアウトを設定可能にする by @rokoucha in https://github.com/rokoucha/Mahiron/pull/116
- EPGStation との互換性を修正 (genres の省略 / path-level parameters) by @rokoucha in https://github.com/rokoucha/Mahiron/pull/117
- Update OpenTelemetry modules together to v1.45.0/v0.21.0 by @rokoucha in https://github.com/rokoucha/Mahiron/pull/118
- Pin golangci-lint and stop serving its cache across versions by @rokoucha in https://github.com/rokoucha/Mahiron/pull/120
- Amortize data broadcast cache pruning by @rokoucha in https://github.com/rokoucha/Mahiron/pull/119
- Serve pprof handlers when observability.pprof is enabled by @rokoucha in https://github.com/rokoucha/Mahiron/pull/121
- Run the data broadcast cache at synchronous=NORMAL by @rokoucha in https://github.com/rokoucha/Mahiron/pull/122
- Drop packets for a slow subscriber instead of disconnecting it by @rokoucha in https://github.com/rokoucha/Mahiron/pull/123
- Update dependency vitest to v4.1.11 by @renovate[bot] in https://github.com/rokoucha/Mahiron/pull/124
- Update dependency vite to v8.2.2 by @renovate[bot] in https://github.com/rokoucha/Mahiron/pull/125
- Update dependency @vitejs/plugin-react to v6.1.0 by @renovate[bot] in https://github.com/rokoucha/Mahiron/pull/126
- Update dependency golangci/golangci-lint to v2.13.1 by @renovate[bot] in https://github.com/rokoucha/Mahiron/pull/128
- Update dependency eslint to v10.9.0 by @renovate[bot] in https://github.com/rokoucha/Mahiron/pull/127

## [v5.0.15](https://github.com/rokoucha/Mahiron/compare/v5.0.14...v5.0.15) - 2026-08-09

- Update dependency typescript-eslint to v8.66.0 by @renovate[bot] in https://github.com/rokoucha/Mahiron/pull/89
- Update module ariga.io/atlas to v1.3.0 by @renovate[bot] in https://github.com/rokoucha/Mahiron/pull/90
- Revert "Update module ariga.io/atlas to v1.3.0" by @rokoucha in https://github.com/rokoucha/Mahiron/pull/92
- Skip broken Atlas v1.3.0 update and log migration startup by @rokoucha in https://github.com/rokoucha/Mahiron/pull/94
- Update Atlas to v1.3.0 by @rokoucha in https://github.com/rokoucha/Mahiron/pull/96
- Batch data broadcast cache pruning by @rokoucha in https://github.com/rokoucha/Mahiron/pull/97
- Index data broadcast cache pruning by @rokoucha in https://github.com/rokoucha/Mahiron/pull/98
- Update dependency vite to v8.2.1 by @renovate[bot] in https://github.com/rokoucha/Mahiron/pull/99
- Update module github.com/go-faster/errors to v0.8.0 by @renovate[bot] in https://github.com/rokoucha/Mahiron/pull/100

## [v5.0.14](https://github.com/rokoucha/Mahiron/compare/v5.0.13...v5.0.14) - 2026-08-06

- Safariでfaviconが正しく表示されるようにする by @rokoucha in https://github.com/rokoucha/Mahiron/pull/75
- Update dependency eslint to v10.8.0 by @renovate[bot] in https://github.com/rokoucha/Mahiron/pull/77
- Update dependency globals to v17.8.0 by @renovate[bot] in https://github.com/rokoucha/Mahiron/pull/78
- Update dependency @vitejs/plugin-react to v6.0.5 by @renovate[bot] in https://github.com/rokoucha/Mahiron/pull/79
- Update dependency typescript-eslint to v8.65.0 by @renovate[bot] in https://github.com/rokoucha/Mahiron/pull/80
- Update react monorepo by @renovate[bot] in https://github.com/rokoucha/Mahiron/pull/81
- Update dependency vite to v8.2.0 by @renovate[bot] in https://github.com/rokoucha/Mahiron/pull/82
- Update dependency globals to v17.9.0 by @renovate[bot] in https://github.com/rokoucha/Mahiron/pull/83
- Update docker/login-action action to v4.6.0 by @renovate[bot] in https://github.com/rokoucha/Mahiron/pull/84
- Fix session worker restart race condition by @rokoucha in https://github.com/rokoucha/Mahiron/pull/86
- Serve a provisional data-broadcast snapshot before a tuner is acquired by @rokoucha in https://github.com/rokoucha/Mahiron/pull/85
- Recover data-broadcast modules evicted from the persistent cache by @rokoucha in https://github.com/rokoucha/Mahiron/pull/87
- Deliver shared-PID carousel sections to every referencing service by @rokoucha in https://github.com/rokoucha/Mahiron/pull/88

## [v5.0.13](https://github.com/rokoucha/Mahiron/compare/v5.0.12...v5.0.13) - 2026-07-30

- Prevent silent stream fanout drops by @rokoucha in https://github.com/rokoucha/Mahiron/pull/72
- Keep shared stream alive when a peer detaches by @rokoucha in https://github.com/rokoucha/Mahiron/pull/74

## [v5.0.12](https://github.com/rokoucha/Mahiron/compare/v5.0.11...v5.0.12) - 2026-07-28

- コード整理: 生成コードのマーキングと肥大化ファイル・パッケージの分割 by @rokoucha in https://github.com/rokoucha/Mahiron/pull/65
- Update actions/upload-artifact action to v7.0.1 by @renovate[bot] in https://github.com/rokoucha/Mahiron/pull/67
- Update dependency @vitejs/plugin-react to v6.0.4 by @renovate[bot] in https://github.com/rokoucha/Mahiron/pull/68
- Update dependency prettier to v3.9.6 by @renovate[bot] in https://github.com/rokoucha/Mahiron/pull/69
- Update react monorepo to v19.2.8 by @renovate[bot] in https://github.com/rokoucha/Mahiron/pull/70
- Fix data broadcast DDB queue starvation by @rokoucha in https://github.com/rokoucha/Mahiron/pull/71

## [v5.0.11](https://github.com/rokoucha/Mahiron/compare/v5.0.10...v5.0.11) - 2026-07-24

- CIビルドとnightly Dockerイメージを公開する by @rokoucha in https://github.com/rokoucha/Mahiron/pull/61
- Fix data broadcast SSE disconnect loop by @rokoucha in https://github.com/rokoucha/Mahiron/pull/63
- Fix remote data broadcast SSE reconnects by @rokoucha in https://github.com/rokoucha/Mahiron/pull/64

## [v5.0.10](https://github.com/rokoucha/Mahiron/compare/v5.0.9...v5.0.10) - 2026-07-23

- Update actions/checkout action to v7.0.1 by @renovate[bot] in https://github.com/rokoucha/Mahiron/pull/58
- Fix tuner status stream info race by @rokoucha in https://github.com/rokoucha/Mahiron/pull/60

## [v5.0.9](https://github.com/rokoucha/Mahiron/compare/v5.0.8...v5.0.9) - 2026-07-21

- Update dependency vite to v8.1.5 by @renovate[bot] in https://github.com/rokoucha/Mahiron/pull/52
- Update module modernc.org/sqlite to v1.54.0 by @renovate[bot] in https://github.com/rokoucha/Mahiron/pull/53
- Update actions/setup-go action to v7 by @renovate[bot] in https://github.com/rokoucha/Mahiron/pull/55
- Update actions/setup-node action to v7 by @renovate[bot] in https://github.com/rokoucha/Mahiron/pull/56
- Improve data broadcast carousel recovery by @rokoucha in https://github.com/rokoucha/Mahiron/pull/51
- Restore tagpr release state by @rokoucha in https://github.com/rokoucha/Mahiron/pull/57

## [v5.0.8](https://github.com/rokoucha/Mahiron/compare/v5.0.7...v5.0.8) - 2026-07-17

- Update dependency typescript-eslint to v8.64.0 by @renovate[bot] in https://github.com/rokoucha/Mahiron/pull/45
- Update Songmu/tagpr action to v1.20.1 by @renovate[bot] in https://github.com/rokoucha/Mahiron/pull/47
- Update actions/setup-node action to v6.5.0 by @renovate[bot] in https://github.com/rokoucha/Mahiron/pull/48
- Log TS packet drops with rate-limited warnings. by @rokoucha in https://github.com/rokoucha/Mahiron/pull/49
- Handle valid TS continuity discontinuities by @rokoucha in https://github.com/rokoucha/Mahiron/pull/50

## [v5.0.7](https://github.com/rokoucha/Mahiron/compare/v5.0.6...v5.0.7) - 2026-07-13

- Share remote channel streams across subscribers by @rokoucha in https://github.com/rokoucha/Mahiron/pull/36
- Unify local and remote channel sessions by @rokoucha in https://github.com/rokoucha/Mahiron/pull/38
- Remove obsolete internal compatibility layers by @rokoucha in https://github.com/rokoucha/Mahiron/pull/39
- Filter unrelated DSM-CC carousels before queueing by @rokoucha in https://github.com/rokoucha/Mahiron/pull/40
- Improve data broadcast module loading and caching by @rokoucha in https://github.com/rokoucha/Mahiron/pull/41
- 視聴中サービス表示のちらつきを修正 by @rokoucha in https://github.com/rokoucha/Mahiron/pull/42
- Update dependency eslint to v10.7.0 by @renovate[bot] in https://github.com/rokoucha/Mahiron/pull/43
- Update module github.com/ogen-go/ogen to v1.23.0 by @renovate[bot] in https://github.com/rokoucha/Mahiron/pull/44

## [v5.0.6](https://github.com/rokoucha/Mahiron/compare/v5.0.5...v5.0.6) - 2026-07-12

- Improve data broadcast event support by @rokoucha in https://github.com/rokoucha/Mahiron/pull/33
- Encode data broadcast byte fields as number arrays by @rokoucha in https://github.com/rokoucha/Mahiron/pull/35

## [v5.0.5](https://github.com/rokoucha/Mahiron/compare/v5.0.4...v5.0.5) - 2026-07-11

- Update dependency @types/node to v24.13.3 by @renovate[bot] in https://github.com/rokoucha/Mahiron/pull/21
- Update dependency prettier to v3.9.5 by @renovate[bot] in https://github.com/rokoucha/Mahiron/pull/24
- Update dependency vitest to v4.1.10 by @renovate[bot] in https://github.com/rokoucha/Mahiron/pull/18
- Update dependency vite to v8.1.4 by @renovate[bot] in https://github.com/rokoucha/Mahiron/pull/23
- Update dependency typescript-eslint to v8.63.0 by @renovate[bot] in https://github.com/rokoucha/Mahiron/pull/19
- Update module golang.org/x/sync to v0.22.0 by @renovate[bot] in https://github.com/rokoucha/Mahiron/pull/20
- Update module github.com/go-co-op/gocron/v2 to v2.22.0 by @renovate[bot] in https://github.com/rokoucha/Mahiron/pull/25
- Fix remote Mirakurun service requests by @rokoucha in https://github.com/rokoucha/Mahiron/pull/27
- Update module golang.org/x/text to v0.40.0 by @renovate[bot] in https://github.com/rokoucha/Mahiron/pull/17
- Fix EPG gathering after service scans by @rokoucha in https://github.com/rokoucha/Mahiron/pull/28
- Fix remote logo persistence by @rokoucha in https://github.com/rokoucha/Mahiron/pull/29
- Show configured remote tuner status by @rokoucha in https://github.com/rokoucha/Mahiron/pull/30
- Show local users on remote tuners by @rokoucha in https://github.com/rokoucha/Mahiron/pull/31
- Support data broadcasts from remote streams by @rokoucha in https://github.com/rokoucha/Mahiron/pull/32

## [v5.0.4](https://github.com/rokoucha/Mahiron/compare/v5.0.3...v5.0.4) - 2026-07-05

- Protect service logos during refresh by @rokoucha in https://github.com/rokoucha/Mahiron/pull/14
- Add data broadcast API by @rokoucha in https://github.com/rokoucha/Mahiron/pull/16

## [v5.0.3](https://github.com/rokoucha/Mahiron/compare/v5.0.2...v5.0.3) - 2026-07-05

## [v5.0.2](https://github.com/21S1298001/Mahiron5/compare/v5.0.1...v5.0.2) - 2026-07-04

- Fix DSM-CC DDB parsing so BS/CS common logos are assembled by @rokoucha in https://github.com/21S1298001/Mahiron5/pull/11

## [v5.0.1](https://github.com/21S1298001/Mahiron5/compare/v5.0.0...v5.0.1) - 2026-07-04

- Update dependency typescript to v6 by @renovate[bot] in https://github.com/21S1298001/Mahiron5/pull/6
- Update dependency vite to v8 by @renovate[bot] in https://github.com/21S1298001/Mahiron5/pull/7
- Update dependency @vitejs/plugin-react to v6 by @renovate[bot] in https://github.com/21S1298001/Mahiron5/pull/5
- Switch OTLP exporters from gRPC to HTTP by @rokoucha in https://github.com/21S1298001/Mahiron5/pull/9
- Fix OTLP log exports failing with 404 on path-less endpoints by @rokoucha in https://github.com/21S1298001/Mahiron5/pull/10

## [v5.0.0](https://github.com/21S1298001/Mahiron5/commits/v5.0.0) - 2026-07-03
