// 접속과 연결 실패에 붙이는 고정 사유. ws가 붙이는 고정 문구와 Node의 오류 코드만 보고 주소, 헤더, 본문은 읽지 않는다.
export const REASONS = ['handshake-timeout', 'connect-aborted', 'rejected-502', 'rejected-503', 'rejected-other', 'connect-refused', 'connect-reset', 'connect-error', 'connection-error']

function classify(error, opened) {
  // 연결된 뒤의 소켓 오류는 ws가 error 없이 닫으므로 여기에는 프로토콜 오류만 온다.
  if (opened) return 'connection-error'
  if (error.message === 'Opening handshake has timed out') return 'handshake-timeout'
  // 클라이언트가 handshake 중인 소켓을 스스로 끊으면 ws가 붙이는 문구다. 예제 클라이언트에서는 수신 기한 3초가 그 원인이다.
  if (error.message === 'WebSocket was closed before the connection was established') return 'connect-aborted'
  const status = /^Unexpected server response: (\d{3})$/.exec(error.message)?.[1]
  if (status) return status === '502' || status === '503' ? `rejected-${status}` : 'rejected-other'
  if (error.code === 'ECONNREFUSED') return 'connect-refused'
  if (error.code === 'ECONNRESET') return 'connect-reset'
  return 'connect-error'
}

// 소켓 하나의 첫 오류를 사유로 기억한다. 오류 없이 닫히면 null이다.
export function watch(socket) {
  let opened = false, reason = null
  socket.once('open', () => { opened = true })
  socket.on('error', error => { reason ??= classify(error, opened) })
  return () => reason
}
