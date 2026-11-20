use super::*;

impl State {
    pub(super) fn subscribe(&mut self, conn: u64, id: String, destination: String, receipt: Option<String>) {
        let Some(client) = self.clients.get_mut(&conn) else {
            return;
        };
        if client.closing {
            return;
        }
        let owner = personal_owner(&destination);
        let error = if !client.rate.allow(self.limits.commands_per_second) {
            Some("rate-limit-exceeded")
        } else if owner.is_some_and(|owner| owner.is_empty() || client.principal.as_deref() != Some(owner)) {
            // 연결을 닫는 reject 대신 이 구독만 버린다. 사유를 하나로 두어 거절 응답으로 다른 주체의 존재를 추측하지 못하게 한다.
            tracing::debug!(event = "subscription_rejected", conn, reason = "forbidden-topic");
            if let Some(receipt_id) = receipt {
                self.deliver(conn, Outgoing::Denied { receipt_id, code: "forbidden-topic" });
            }
            return;
        } else if owner.is_none() && !self.limits.topics.is_empty() && !self.limits.topics.contains(&destination) {
            Some("unknown-topic")
        } else if self.limits.subscriptions > 0
            && client.subs.len() >= self.limits.subscriptions
            && !client.subs.contains_key(id.as_str())
        {
            Some("too-many-subscriptions")
        } else {
            None
        };
        if let Some(code) = error {
            tracing::debug!(event = "subscription_rejected", conn, reason = code);
            self.reject(conn, code);
            return;
        }
        let id: Arc<str> = id.into();
        if let Some(old) = client.subs.insert(id.clone(), destination.clone()) {
            self.unindex(conn, &id, &old);
        }
        self.index.entry(destination).or_default().push((conn, id));
        // 색인에 올린 바로 이 자리에서 RECEIPT를 큐에 넣는다. 이후 발행분은 항상 그 뒤에 쌓인다.
        if let Some(receipt) = receipt {
            self.deliver(conn, Outgoing::Receipt { receipt_id: receipt, message: "subscribe success" });
        }
    }

    // 색인에서 빼는 것과 RECEIPT를 큐에 넣는 것이 한 명령 안에서 일어난다.
    // 이미 큐에 있던 MESSAGE는 RECEIPT보다 앞에 있고, 이후 발행분은 이 구독으로 오지 않는다.
    // 구독 중이 아닌 id도 관측이 없어 같은 RECEIPT로 답한다.
    pub(super) fn unsubscribe(&mut self, conn: u64, id: &str, receipt: Option<String>) {
        let Some(client) = self.clients.get_mut(&conn) else { return };
        if client.closing {
            return;
        }
        if !client.rate.allow(self.limits.commands_per_second) {
            tracing::debug!(event = "subscription_rejected", conn, reason = "rate-limit-exceeded");
            self.reject(conn, "rate-limit-exceeded");
            return;
        }
        if let Some(topic) = client.subs.remove(id) {
            self.unindex(conn, id, &topic);
        }
        if let Some(receipt) = receipt {
            self.deliver(conn, Outgoing::Receipt { receipt_id: receipt, message: "unsubscribe success" });
        }
    }

    pub(super) fn publish(&mut self, topic: &str, body: Arc<str>) {
        self.published += 1;
        self.cache.record(topic, body.clone());
        let Some(targets) = self.index.get(topic) else {
            return;
        };
        let mut full = Vec::new();
        for (conn, id) in targets {
            let Some(client) = self.clients.get(conn) else {
                continue;
            };
            if client.closing {
                continue;
            }
            let message = Outgoing::Message { subscription: id.clone(), body: body.clone() };
            match client.queue.try_send(message) {
                Ok(()) => self.enqueued += 1,
                Err(TrySendError::Full(_)) => full.push(*conn),
                Err(TrySendError::Closed(_)) => {}
            }
        }
        for conn in full {
            self.drop_slow(conn);
        }
    }

    fn reject(&mut self, conn: u64, code: &'static str) {
        let Some(client) = self.clients.get_mut(&conn) else { return };
        client.closing = true;
        // 송신 큐가 막혀도 연결 쪽에서 종료 기한을 시작할 수 있어야 한다.
        if let Some(kick) = client.kick.take() {
            let _ = kick.send(code);
        }
        self.deliver(conn, Outgoing::Error(code));
    }

    pub(super) fn deliver(&mut self, conn: u64, outgoing: Outgoing) {
        let Some(client) = self.clients.get(&conn) else {
            return;
        };
        match client.queue.try_send(outgoing) {
            Ok(()) => self.enqueued += 1,
            Err(TrySendError::Full(_)) => self.drop_slow(conn),
            Err(TrySendError::Closed(_)) => {}
        }
    }

    pub(super) fn drop_slow(&mut self, conn: u64) {
        if self.clients.contains_key(&conn) {
            self.slow_disconnections += 1;
            tracing::debug!(event = "slow_consumer_dropped", conn);
            self.remove(conn);
        }
    }

    pub(super) fn remove(&mut self, conn: u64) {
        let Some(client) = self.clients.remove(&conn) else {
            return;
        };
        for (id, topic) in client.subs {
            self.unindex(conn, &id, &topic);
        }
    }

    pub(super) fn unindex(&mut self, conn: u64, id: &str, topic: &str) {
        let Some(list) = self.index.get_mut(topic) else {
            return;
        };
        list.retain(|(c, i)| !(*c == conn && &**i == id));
        if list.is_empty() {
            self.index.remove(topic);
        }
    }
}
