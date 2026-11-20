# ws-trade-server 작업 안내

증권 체결 웹소켓 서버를 Rust로 직접 세워 보는 실험 저장소다. 코드는 블로그 글에 인용되므로 짧고 읽기 쉬워야 한다. 배경과 실행 방법은 `README.md`에 있다.

## 검증

코드를 바꾸면 저장소 루트에서 아래가 모두 통과해야 한다.

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```

프레임 모양을 건드렸다면 `README.md`의 "캡처와 대조하기" 절차로 `contract/wts-stomp.json`과 비교한다.

`capture/extract-contract.mjs`를 바꾸면 합성 캡처 테스트도 통과해야 한다.

```sh
node --test capture/extract-contract.test.mjs
```

## Rust 스킬

Rust 코드를 쓰거나 검토할 때 `rust-skills` 스킬을 쓴다. Codex는 `.agents/skills/rust-skills/`, Claude Code는 같은 폴더를 가리키는 `.claude/skills/rust-skills`에서 읽는다. 규칙이 265개라 전부 볼 필요는 없고, 이 저장소에서는 아래 범주를 먼저 본다.

- `async-`: tokio 태스크, 채널, `select!`, 취소 안전성
- `conc-`, `unsafe-`: 원자적 연산의 순서 보장, 공유 메모리에 접근하는 `unsafe` 블록
- `err-`, `own-`, `anti-`: 오류 처리, 불필요한 복제, 흔한 실수
- `perf-`, `mem-`: 팬아웃 경로의 할당과 복사
- `test-`, `lint-`: 테스트 구성과 clippy 설정

작업 지시서(`docs/*-brief.md`)와 스킬의 규칙이 어긋나면 지시서를 따르고, 어긋난 지점을 보고한다.

## 지켜야 할 것

- 근거는 공개 자료만 쓴다. `evidence/raw/`의 값(토큰, 실제 체결 값)을 코드, 테스트, 문서에 옮기지 않는다.
- `contract/`는 `capture/extract-contract.mjs`가 만든 결과물이다. 손으로 고치지 않는다.
- 관측하지 않은 동작을 상상해서 넣지 않는다. 넣어야 하면 README의 "실측이 없어 넣은 것" 목록에 적는다.
- 커밋, 푸시는 사람이 요청했을 때만 한다.
- 주석은 코드만 봐서는 알 수 없는 이유에만, 한국어로 단다. 주석과 문서에 em dash, en dash, 가운뎃점을 쓰지 않고, 문서의 한 문단은 한 줄로 쓴다.
