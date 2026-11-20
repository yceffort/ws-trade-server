# HA 공통 인증

선택적 `ha` feature의 `/experimental/ha/ws`는 HTTPS 정책 공급자와 RS256 access token을 사용하는 실험 프로파일을 제공한다. 실제 증권사 인증을 재현한 것이 아니다. `--ha-policy`를 지정한 gateway는 이 경로만 허용하며 별도 프로세스의 기존 JSON token 및 `/ws` 모드는 유지한다. 정책 공급자는 운영자가 별도로 구성해야 하며 가용성과 데이터 일관성도 운영 책임에 포함된다. 저장소의 `tests/ha-policy-provider.mjs`는 합성 시험 전용이다.

## 설정과 토큰

`gateway --ha-kafka gateway.json --ha-source source.json --ha-cursor-keys cursor-keys.json --ha-policy policy-client.json`으로 실행한다. 정확한 broker 실행 예시는 [복제 로그 안내](../deploy/ha/README.md)를 따른다. `policy-client.json`의 예시는 다음과 같다. CA와 공급자 호출 credential은 필요할 때만 지정한다.

```json
{"url":"https://policy.example.test/policy","issuer":"https://issuer.example.test","audience":"wts-ha","ca_file":null,"credential_file":null,"max_token_seconds":900}
```

공급자 credential 파일에는 Bearer 접두사 없이 공급자가 허용한 credential 값을 넣으며 로그에 출력하지 않는다. HTTPS만 허용하고 redirect와 환경 HTTP proxy를 사용하지 않는다. 요청은 2초, 응답은 1 MiB, 키는 8개, subject는 10,000개, topic은 subject당 256개, 폐기 목록은 8,192개로 제한한다. 제한을 넘는 운영 규모에는 공급자 계약과 구현을 먼저 확장한다.

connect 프레임의 `token`은 `Bearer <JWT>`다. header는 `alg=RS256`, `typ=at+jwt`, 등록된 `kid`만 허용한다. claims는 문자열 `iss`, 문자열 `aud`, `sub`, `jti`, 초 단위 정수 `iat`, `nbf`, `exp`, 정렬되고 중복 없는 `topics` 배열을 사용한다. 이 제한된 프로파일 밖의 claim/header는 거절한다. RSA 2048~4096 bit, 지수 65537을 사용하며 토큰 수명 상한은 설정의 60~3,600초다. 서명, issuer, audience, 만료, 현재 scope를 모두 검증하고 연결 중에도 exp와 정책을 검사한다. JWT와 전체 cursor를 로그에 남기지 않는다.

cursor는 `issuer + NUL + subject`와 요청 topic 집합에 묶인다. 재접속마다 현재 토큰과 최신 정책으로 topic 권한을 다시 검증한다. 다른 subject, issuer 또는 scope의 cursor는 공유할 수 없다. 기존 로컬 JSON 인증과 shared JWT의 subject 형식이 다르므로 두 프로파일 사이에서 cursor를 이전하지 않는다.

## 정책 공급자 계약

GET 응답은 다음 구조다. `version`은 증가하는 십진 문자열이며 동일 version에는 동일한 policy만 허용한다. `generated_at`은 요청 시점에서 5초 이내, 미래 오차는 최대 1초, `valid_until`은 generated_at 이후 최대 55초다. 공급자는 원본 정책을 확인한 경우에만 새 freshness envelope를 발급해야 한다. 오래된 replica의 정책을 현재 시각으로 포장하면 폐기 보장은 성립하지 않는다. TLS 인증과 공급자 내부의 일관된 version 발급이 필요하다.

```json
{"generated_at":1795000000,"valid_until":1795000055,"policy":{"version":"42","keys":[{"kid":"key-2026-11","kty":"RSA","alg":"RS256","use":"sig","n":"<base64url RSA modulus>","e":"AQAB","retain_until":1796000000}],"subjects":{"alice":{"generation":"3","topics":["/topic/v1/kr/stock/trade/A005930"]}},"revoked":{"revoked-token-id":{"revoked_at":1795000000,"retain_until":1795000900}}}}
```

gateway는 5초마다 갱신한다. 실패 시 마지막 정상 캐시의 남은 수명만 허용하며 wall clock 만료와 요청 시작부터의 monotonic 55초 상한을 함께 확인한다. 활성 세션은 250 ms 간격과 정책 변경 알림으로 검사한다. 공급자 장애로 새 폐기를 받지 못해도 마지막 확인 후 55초가 지나면 새 연결을 거절하고 readiness를 내리며 기존 세션에 `auth-policy-stale`을 보낸다. 이는 60초 내 반영 또는 fail closed 목표의 여유를 포함한다. bootstrap에서 정책을 얻지 못하면 ready가 되지 않는다. 공급자 한 번의 실패만으로 fresh cache를 버리지는 않는다.

`revoked`는 jti에서 폐기 발생 시각 revoked_at과 보존 종료 retain_until로의 매핑이다. gateway의 최초 수신 시각 대신 revoked_at부터 max_token_seconds 이상 보존하고 기존 폐기의 보존 시간을 줄이거나 아직 유효한 항목을 제거할 수 없다. 만료한 항목만 정리한다. topic 권한 변경 또는 subject 삭제는 기존 세션을 폐기하며 빠르게 삭제 후 복구해도 이전 세션이 살아나지 않는다. 공급자는 subject별 증가하는 generation을 권한 변경 및 삭제/복구 시마다 올리고 중간 version을 건너뛴 gateway도 이를 확인할 수 있도록 보존한다. topic이 같아도 generation이 달라지면 이전 세션을 종료한다. 같은 generation에서 topic을 바꾸거나 generation을 낮추는 정책을 거절한다. 삭제한 subject를 다시 사용할 때 이전보다 큰 generation을 발급해야 한다. 폐기된 세션은 현재 권한으로 다시 인증해야 한다. 토큰 자체를 폐기하려면 jti도 revoked에 기록한다.

## 키 회전과 배포

새 JWT 검증 키를 policy에 먼저 추가하고 모든 gateway가 받아들였는지 시험한 뒤 공급자의 서명 키를 바꾼다. unknown kid는 임의 URL 조회나 요청별 refresh를 만들지 않고 거절한다. `retain_until` 전에는 기존 키를 삭제하거나 같은 kid의 값을 변경할 수 없다. 이 시각은 마지막 발급 토큰의 만료보다 늦어야 한다. 정책 version과 공급자 상태를 이전 버전으로 되돌리는 rollback은 허용하지 않는다.

cursor 키는 JWT 키와 별개다. 모든 gateway에 새 검증 키를 먼저 배포한 다음 current key를 바꾸며 이전 키는 마지막 cursor 발급 이후 24시간 이상 보존한다. 현재 cursor 키가 새 24시간 cursor의 수명을 보장하지 못하면 readiness가 내려간다. cursor 키 파일은 시작 시 읽으므로 version이 있는 immutable 설정과 직렬 rollout을 사용한다. projected Secret 갱신을 즉각적인 폐기 수단으로 사용하지 않는다.

## 검증과 근거

단위 시험은 변조, algorithm/issuer/audience/expiry/scope 오류, 정책 rollback과 version 충돌, 빠른 권한 변경, 두 gateway의 키 사전 배포와 이전 키 보존을 검증한다. `deploy/ha/smoke.sh`는 별도 HTTPS 공급자의 폐기 및 장애, Kafka 기반 세 gateway 간 signed cursor 복구와 10,040개 committed event 적용 대조, 모든 활성 gateway의 세션 폐기를 검사한다. test fixture 개인 키는 이 시험만을 위해 생성한 공개 합성 데이터이며 운영에 사용하지 않는다.

형식과 검증 원칙은 [JWT 보안 권고 RFC 8725](https://www.rfc-editor.org/rfc/rfc8725.html), [JWK RFC 7517](https://www.rfc-editor.org/rfc/rfc7517.html), [ring RSA 검증 API](https://docs.rs/ring/0.17.14/ring/signature/struct.RsaPublicKeyComponents.html)를 참조했다. 실제 인증 공급자의 다중 장애 도메인과 운영 지연은 별도로 검증해야 한다.
