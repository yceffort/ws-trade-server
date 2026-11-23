# ws-trade-server 작업 안내

증권 체결 웹소켓 서버를 Rust로 직접 세워 보는 실험 저장소다. 코드는 블로그 글에 인용되므로 짧고 읽기 쉬워야 한다. 배경과 실행 방법은 `README.md`에 있다. 작업의 명세는 GitHub 이슈 본문이다. `docs/m1-brief.md`부터 `docs/m3-brief.md`까지는 이슈를 쓰기 전에 만든 첫 세 단계의 작업 지시서다.

## 검증

Rust 코드를 바꾸면 저장소 루트에서 아래가 모두 통과해야 한다. `ha` feature를 켠 구성과 끈 구성은 서로 다른 코드를 컴파일하므로 둘 다 돌린다. Rust 판은 `rust-toolchain.toml`이 CI와 같은 판으로 고정한다.

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
cargo clippy --features ha --all-targets -- -D warnings
cargo test --features ha
```

프레임 모양을 건드렸다면 `README.md`의 "캡처와 대조하기" 절차로 `contract/wts-stomp.json`과 비교한다.

JavaScript를 바꾸면 아래가 통과해야 한다. 도구는 `npm ci`로 설치하고, 포맷은 `npm run format`으로 맞춘다.

```sh
npm run lint
npm run format:check
```

바꾼 곳에 따라 아래 테스트도 통과해야 한다. Node 테스트는 `npm ci --prefix edge --ignore-scripts`로 `ws`를 설치한 뒤에 돈다.

| 바꾼 곳 | 명령 |
| --- | --- |
| `capture/extract-contract.js` | `node --test capture/extract-contract.test.js` |
| `examples/` | `node --test examples/*.test.js` |
| `edge/` | `npm test --prefix edge` |
| `probe/`, `bench/soak/`, 연결과 구독의 동작 | `cargo build --bin gateway --bin feed` 뒤에 `node tests/websocket-e2e.test.js`, `node --test probe/*.test.js` |
| `bench/soak.js`, `bench/soak/` | 윗줄의 빌드 뒤에 `node bench/soak.js --seconds 6 --cycles 2 --connections 4 --bin-dir target/debug --out /tmp/soak.json` |
| `bench/ha/`, `deploy/k8s/ha/` | `node --test bench/ha/*.test.js deploy/k8s/ha/*.test.js` |
| `deploy/k8s/ha/observability/` | `node deploy/k8s/ha/observability/dashboard-check.js` |

PR을 열거나 `main`에 푸시하면 `.github/workflows/ci.yml`이 위 명령을 Linux에서 다시 돌리고 힙 링 전용 Miri 테스트도 실행한다. 다만 `ha` feature 명령과 표의 마지막 두 줄은 PR에서 `ha.yml`이 돌린다. Docker나 클러스터가 필요한 검증(이미지 빌드, `deploy/ha/smoke.sh`, promtool, kind)은 `ha.yml`과 `deploy.yml`이 맡는다. 의존성은 `package-lock.json`과 `edge/package-lock.json`으로 설치하며 `ci.yml`의 각 작업은 실행 시간을 20분으로 제한한다.

## Rust 스킬

Rust 코드를 쓰거나 검토할 때 `rust-skills` 스킬을 쓴다. Codex는 `.agents/skills/rust-skills/`, Claude Code는 같은 폴더를 가리키는 `.claude/skills/rust-skills`에서 읽는다. 규칙이 265개라 전부 볼 필요는 없고, 이 저장소에서는 아래 범주를 먼저 본다.

- `async-`: tokio 태스크, 채널, `select!`, 취소 안전성
- `conc-`, `unsafe-`: 원자적 연산의 순서 보장, 공유 메모리에 접근하는 `unsafe` 블록
- `err-`, `own-`, `anti-`: 오류 처리, 불필요한 복제, 흔한 실수
- `perf-`, `mem-`: 팬아웃 경로의 할당과 복사
- `test-`, `lint-`: 테스트 구성과 clippy 설정

이슈나 작업 지시서(`docs/*-brief.md`)가 스킬의 규칙과 어긋나면 이슈나 지시서를 따르고, 어긋난 지점을 보고한다.

## JavaScript

JavaScript는 ES 모듈로 쓰고 확장자는 `.js`다. 루트와 `edge/`의 `package.json`이 `"type": "module"`을 선언한다. 린트는 oxlint, 포맷은 oxfmt가 맡는다. `.oxfmtrc.json`은 JavaScript 파일만 포맷하게 막아 두었다.

ConfigMap으로 Pod에 올리는 스크립트(`deploy/k8s/ha/`의 `dev-policy.js`, `cluster-client.js`, `metrics-proxy.js`, `observability/alert-receiver.js`)는 `package.json`이 없는 경로에서 실행된다. Node 24는 그런 `.js`를 모듈 문법(`import`, `export` 등)이 있을 때만 ES 모듈로 실행하므로, 이 스크립트에서 모듈 문법이 사라지면 CommonJS로 바뀐다.

## 지켜야 할 것

- 근거는 공개 자료만 쓴다. `evidence/raw/`의 값(토큰, 실제 체결 값)을 코드, 테스트, 문서에 옮기지 않는다.
- `contract/`는 `capture/extract-contract.js`가 만든 결과물이다. 손으로 고치지 않는다.
- 관측하지 않은 동작을 상상해서 넣지 않는다. 넣어야 하면 README의 "실측이 없어 넣은 것" 목록에 적는다.
- 커밋, 푸시는 사람이 요청했을 때만 한다.
- `deploy/aws/`의 스크립트와 `aws`, `eksctl` 명령은 요금이 붙는 실제 자원을 만들거나 지운다. 사람이 요청했을 때만 실행한다. Claude Code에서는 `.claude/settings.json`이 `deploy/aws/` 스크립트와 `eksctl create`, `eksctl delete`를 실행하기 전에 확인을 받는다.
- 주석은 코드만 봐서는 알 수 없는 이유에만, 한국어로 단다. 주석과 문서에 em dash, en dash, 가운뎃점을 쓰지 않고, 문서의 한 문단은 한 줄로 쓴다.
