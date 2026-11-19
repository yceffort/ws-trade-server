use serde_json::{Map, Value};

pub fn contract() -> Value {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/contract/wts-stomp.json");
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

// 본문 키 순서와 값의 타입(문자열, 정수, 소수)을 계약의 bodies와 대조한다.
pub fn assert_body_shape(contract: &Value, pattern: &str, body: &str) {
    let shape = &contract["server"]["bodies"][pattern];
    let expected_keys: Vec<&str> =
        shape["keyOrders"][0].as_array().unwrap().iter().map(|k| k.as_str().unwrap()).collect();
    let actual: Map<String, Value> = serde_json::from_str(body).unwrap();
    assert_eq!(actual.keys().collect::<Vec<_>>(), expected_keys, "{body}");

    for (key, value) in &actual {
        let field = &shape["fields"][key];
        let kind = match value {
            Value::String(_) => "string",
            Value::Number(n) if n.is_f64() => "decimal",
            Value::Number(_) => "integer",
            other => panic!("예상 밖 타입 {key}: {other}"),
        };
        assert!(field["types"].as_array().unwrap().iter().any(|t| t == kind), "{key} 타입 {kind}: {body}");
        // 어휘는 모든 topic에서 관측된 값의 합집합으로 확인한다.
        let observed: Vec<&Value> = contract["server"]["bodies"]
            .as_object()
            .unwrap()
            .values()
            .filter_map(|entry| entry["fields"][key]["values"].as_array())
            .flatten()
            .collect();
        if !observed.is_empty() {
            assert!(observed.contains(&value), "{key} 어휘: {body}");
        }
    }
    let dt = actual["dt"].as_str().unwrap();
    assert!(dt.len() == 20 && chrono::DateTime::parse_from_rfc3339(dt).is_ok(), "dt 형식: {dt}");
}
