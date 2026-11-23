# 근거 자료

원본은 `raw/`에 있고 커밋하지 않는다. 아래 주소에서 다시 받아 해시로 대조할 수 있다.

| 파일 | 출처 | 버전 | 받은 시각 | sha256 |
| --- | --- | --- | --- | --- |
| `raw/asyncapi-1.2.3.json` | <https://openapi.tossinvest.com/openapi-docs/latest/asyncapi.json> | AsyncAPI 3.0.0, `info.version` 1.2.3 | 2026-11-19 13:46 KST | `46669a360d4c5c6485c157624825e599f5dc55f8b9f66b9b3bcf3bfc3b9e7512` |
| `raw/openapi-1.2.28.json` | <https://openapi.tossinvest.com/openapi-docs/latest/openapi.json> | `info.version` 1.2.28 | 2026-11-19 13:46 KST | `c5918961335ccd6b091577e307dc06ed505b58f83e4d99f0c4617de14ceb036a` |
| `raw/frames-005930-headless.jsonl` | `node capture/cdp-capture.js https://www.tossinvest.com/stocks/A005930/order --seconds 25` | 로그인하지 않은 임시 프로필, 헤드리스 Chrome Beta 159 | 2026-11-19 13:55 KST | 다시 받을 수 없는 캡처라 해시 없음 |

프레임 캡처에는 비로그인 상태에서도 `CONNECT` 프레임의 `authorization`, `device-id` 값이 들어 있다. 글이나 커밋에 옮길 때는 지운다.
