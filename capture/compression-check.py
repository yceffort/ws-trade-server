#!/usr/bin/env python3
# cdp-capture.js가 남긴 프레임(압축을 푼 텍스트)과 Chrome NetLog의 수신 프레임 헤더(압축된 크기)를 받은 순서대로 짝짓는다.
# 서버가 실제로 압축해 보냈는지, 그리고 zlib을 어떤 설정으로 이어 쓰면 프레임마다 같은 크기가 나오는지 센다.
# 바이트가 아니라 크기만 비교한다. 출력에는 개수와 크기만 있고 프레임 내용은 옮기지 않는다.
# Node가 아니라 Python인 이유: Node에 들어 있는 zlib(Chromium 계열)은 같은 입력에서 시스템 zlib과 다른 압축 결과를 낸다.
# 사용: python3 capture/compression-check.py evidence/raw/netlog.json evidence/raw/frames.jsonl
import json
import statistics
import sys
import zlib

netlog = json.load(open(sys.argv[1]))
recv = netlog['constants']['logEventTypes']['WEBSOCKET_RECV_FRAME_HEADER']
by_source = {}
for event in netlog['events']:
    if event['type'] == recv:
        by_source.setdefault(event['source']['id'], []).append({'time': float(event['time']), **event['params']})

frames = [json.loads(line) for line in open(sys.argv[2]) if line.strip()]
frames = [f for f in frames if f.get('event') == 'webSocketFrameReceived']
# NetLog의 source id와 CDP의 requestId를 잇는 정보가 없어 받은 순서로만 짝짓는다. 그래서 연결이 하나인 캡처만 받는다.
if len(by_source) != 1 or len({f['requestId'] for f in frames}) != 1:
    sys.exit('need a capture with exactly one WebSocket connection')
(headers,) = by_source.values()
# NetLog는 프레임마다, CDP는 메시지마다 남긴다. 제어 프레임(ping, close)은 메시지가 아니라서 CDP에 없다.
headers = [h for h in headers if h['opcode'] < 8]
if not all(h['final'] for h in headers):
    sys.exit('fragmented messages cannot be paired with CDP frames')
texts = [f['response']['payloadData'].encode() for f in frames]
pairs = min(len(headers), len(frames))
sizes = [int(h['payload_length']) for h in headers[:pairs]]


# NetLog의 time은 ms, CDP의 timestamp는 초 단위인 같은 단조 시계다
def offset(i):
    return f"{headers[i]['time'] - frames[i]['timestamp'] * 1000:.1f}"


# RFC 7692대로 메시지마다 Z_SYNC_FLUSH로 끊고 끝의 00 00 ff ff 4바이트를 뺀다
def compressed(text, deflate):
    return len(deflate.compress(text) + deflate.flush(zlib.Z_SYNC_FLUSH)) - 4


print(f'zlib {zlib.ZLIB_RUNTIME_VERSION}')
print(f"netlog frames: {len(headers)}, compressed (rsv1): {sum(h['reserved1'] for h in headers)}")
print(f'cdp frames: {len(frames)}, paired: {pairs}, time offset first/last ms: {offset(0)} / {offset(pairs - 1)}')
messages = [i for i in range(pairs) if texts[i].startswith(b'MESSAGE')]
reset = [compressed(texts[i], zlib.compressobj(wbits=-15)) for i in messages]
print(
    f'MESSAGE: {len(messages)}, median text {statistics.median(len(texts[i]) for i in messages)} B, '
    f'median on wire {statistics.median(sizes[i] for i in messages)} B, '
    f'median if the dictionary is reset per message {statistics.median(reset)} B'
)

# 사전을 연결 내내 이어 쓴다. MESSAGE뿐 아니라 CONNECTED, RECEIPT, heartbeat도 받은 순서대로 넣는다.
# 압축하지 않은 메시지(RSV1 꺼짐)는 넣지 않는다. 받는 쪽 압축 해제기가 보지 못한 내용이 서버 사전에 있으면 다음 메시지를 풀 수 없다.
deflated = [(text, size) for text, size, h in zip(texts, sizes, headers) if h['reserved1']]
for wbits in (15, 12, 10):
    row = []
    for level in range(1, 10):
        deflate = zlib.compressobj(level, zlib.DEFLATED, -wbits)
        same = sum(compressed(text, deflate) == size for text, size in deflated)
        row.append(f'L{level} {same}')
    print(f"window 2^{wbits}, compressed frames with the same size of {len(deflated)}: {', '.join(row)}")
