# リモートチューナー

`mahiron-agent` をチューナーPCで動かし、チャンネルとチューナーの設定はMahiron本体だけに置きます。エージェントは本体へ接続するため、チューナーPCの待受ポートは不要です。

エージェントは受信とTSMF分離を行い、未解除TSまたはMMT/TLVを本体へ送ります。B25/B61は本体のローカルカードで実行します。カードのリモート利用・エージェント側の解除には対応しません。

## 設定

エージェントPCに受信コマンド、ドライバー、デバイスへのアクセス権を用意します。エージェントのバイナリは本体と同じバージョンを使ってください。

```sh
mahiron-agent -generate-token
```

表示された `tokenHash` を本体の `server.yml` に登録します。平文の `token` はエージェントPCだけに保存します。

```yaml
agents:
  - name: living
    tokenHash: <表示されたSHA-256値>
```

本体の `tuners.yml`:

```yaml
- name: living-catv
  types: [CATV]
  agent:
    name: living
    command: tuner-command <channel>
    # DVB方式は接続先のパスを指定する
    # dvbDevicePath: /dev/dvb/adapter0/dvr0
  decoder: arib-b25-stream-test
  startupRetryMax: 3
  startupTimeout: 5000
  startupRetryDelay: 1000

- name: living-bs4k
  types: [BS4K]
  agent:
    name: living
    command: tuner-command <channel>
  b61Decoder: arib-b61-stream-test
```

`agent.command` / `agent.dvbDevicePath` はチューナーPCで使用し、最上位の `command` / `dvbDevicePath` とは併用できません。`decoder` / `b61Decoder` は本体で実行します。受信コマンドに `--b25` 等の解除処理を含めないでください。テンプレート変数は本体の既存ルールで展開します。

本体の `channels.yml` はローカル接続と共通です。

```yaml
- name: CATV
  type: CATV
  channel: C013
  tsmfRelTs: 3

- name: BS4K
  type: BS4K
  channel: "101"
  transport: tlv
```

エージェントPCでは、平文トークンだけをファイルに保存し、以下を実行します。チューナー・チャンネル設定ファイルは不要です。

```sh
export MAHIRON_URL=http://mahiron.lan:40772
export MAHIRON_AGENT_TOKEN_FILE=/path/to/agent-token
mahiron-agent
```

`MAHIRON_AGENT_TOKEN` 環境変数も使えます。ファイル指定がある場合はファイルを優先します。トークンの変更・失効には本体設定を変更して本体を再起動します。本体はエージェントPCから到達できるアドレスで待ち受けてください。

## 接続と停止

制御はWebSocket、データは受信セッションごとのHTTP POSTです。どちらも直接接続し、`HTTP_PROXY` 等は使用しません。同じ入力の複数利用者は本体で共有され、最後の利用者が離れると受信を停止します。同一物理受信から異なる相対TSを同時分離する機能はありません。各物理チューナーを一度だけ定義し、本体1台から利用してください。

heartbeatは5秒、15秒応答がなければ接続を失効させます。切断すると旧セッションを停止し、指数バックオフで再接続します。同じIDの二重接続は拒否します。再接続しても録画中のストリームを透過的には再開しません。

起動確認はTSMF分離後に行います。`startupTimeout` は未指定なら2秒で、再試行数0でも有効です。データの読み取りが15秒、送信進捗が30秒止まると終了します。本体側の長い背圧でも受信全体が終了します。

停止を確認できないチューナーは、本体の予約とは別に利用不可状態を保持します。停止完了の確認で解除します。瞬断後は同じエージェントプロセスが旧接続の全受信を回収した証拠を再接続時に送った場合だけ自動解除します。異常終了後の別プロセスでは自動解除しません。制御切断後に回収が確認できなければ、エージェントホストで受信プロセスの終了を確認して本体を再起動してください。

## ネットワークと実行環境

信頼するLAN内なら `http://` で動作します。平文のためトークン・コマンド・受信データの盗聴や改ざんは防げません。本体はエージェント上でコマンドを実行する権限を持ちます。専用OSユーザーで動かし、待受アドレスやファイアウォールでagent用API（`/agent/v1/`）をLANに限定してください。`https://` とWebSocketを中継できるTLS終端も利用できます。

リバースプロキシは制御のWebSocket upgradeと、連続POST本文のバッファリング無効化が必要です。外部のブラウザー向けログイン画面をagent用APIへ適用しないでください。

正常停止時は受信プロセスグループとreaderを回収します。SIGKILL/OOMでの異常終了に備え、ホストではsystemdの `KillMode=control-group` 等、コンテナでは子孫を回収する実行環境を使用してください。シェルからデタッチする受信コマンドは使用しないでください。

## ビルド

```sh
go build -o mahiron-agent ./cmd/mahiron-agent
docker build -f Dockerfile.agent -t mahiron-agent .
```

エージェント用イメージは `ghcr.io/rokoucha/mahiron-agent` です。受信ツールは同梱しないので、ホストのバイナリを使うか派生イメージへ追加してください。コンテナのUID/GIDは10001で、必要なデバイス・グループ・トークンファイルを渡してください。DB・Web UI・フロントエンドのビルドは不要です。

## 実機確認（2026-10-17）

argonのMyGica T230C（adapter3）でC14 / QAM64を受信し、一時エージェントからSSH逆向き転送経由で本体のチューナー・ストリーム管理へ接続して確認しました。既存サービスの設定は変更していません。

| 相対TS | サービス | TS ID | サービスID |
| --- | --- | --- | --- |
| 2 | BS10プレミアム | 16626 | 201 / 800 |
| 3 | BS10 | 18675 | 200 |

各10秒の取得で、TSMFヘッダー残存・セクション解析エラー・TEI・continuity counterエラーは0でした。スクランブル済みパケットは未解除のまま転送されています。停止後のチューナー解放も確認しています。

取得済みの実受信TSMFを再生入力し、argon上で `agent → HTTP → channel.Session → ローカルb25-stream` の実装経路も検証しました。相対TS2・3ともB25解除後のスクランブル済みパケットとセクション解析エラーは0で、MPEG-2映像・AAC音声のデコードを確認しました。検証用本体をカードのあるargon上で動かしており、カードのリモート利用機能は追加していません。

続いて同じ実装経路で物理C14を相対TS2・3それぞれ5分間連続受信しました。両方ともB25前後のTEI・continuity counterエラー・同期ずれ・TSMFヘッダー残存は0、B25後のスクランブル残存も0でした。

短尺で見られた映像・音声の破損系警告は、5分受信でも開始直後と終了端に限定され、途中では発生しませんでした。開始時の映像シーケンスヘッダー待ちと不完全なAAC、終了時のPES・映像・AACの切断によるものです。AACの不完全な先頭・末尾だけを除くと、TS2の14,030フレーム、TS3の14,025フレームがすべて警告なしでデコードできました。TS3の先頭120秒だけをデコードする確認でも終了端の警告は発生しません。FFmpegの未知のデータストリーム等の解析警告は別に残ります。数時間以上の連続運転は未確認です。
