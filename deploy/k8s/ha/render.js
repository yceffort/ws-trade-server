import { readFileSync } from 'node:fs'
import { pathToFileURL } from 'node:url'

const brokerImage = 'apache/kafka:4.1.1@sha256:0bc1bb2478f45b6cea78864df86acdc11e8df2c5172477819a4d12942cbe5d40'
const clone = value => structuredClone(value)
export function render(s) {
  const name = /^[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?$/
  if (
    !name.test(s.namespace) ||
    !name.test(s.version) ||
    s.version.length > 49 ||
    s.zones.length !== 3 ||
    new Set(s.zones).size !== 3 ||
    s.zones.some(z => !name.test(z))
  )
    throw new Error('namespace/version/three distinct zones required')
  if (![s.gatewayImage, s.edgeImage].every(image => /^[\w./:-]+@sha256:[a-f0-9]{64}$/.test(image)))
    throw new Error('image digests required')
  for (const key of ['clientCIDRs', 'gatewayIngressCIDRs', 'policyCIDRs', 'apiCIDRs'])
    if (!Array.isArray(s[key]) || !s[key].length || s[key].some(c => !/^[0-9a-f.:]+\/\d{1,3}$/i.test(c)))
      throw new Error('explicit CIDRs required')
  if (s.metricsNamespace && !name.test(s.metricsNamespace)) throw new Error('invalid metrics namespace')
  // 기본 1GiB는 개발용이다. 목표 처리량에서 24시간을 보관하려면 docs/ha-capacity.md의 산정대로 늘린다.
  const retention = { ms: s.retentionMs ?? 86400000, bytes: s.retentionBytes ?? 1073741824 }
  // gateway 하나를 잃으면 남은 Pod가 연결을 절반씩 더 받는다. edge는 프로세스마다 스레드 하나라 worker 둘로 나눈다(#95).
  // worker마다 연결 상한과 버퍼가 따로 있어 메모리 limit도 함께 늘린다.
  const edgeWorkers = s.edgeWorkers ?? 2
  if (![1, 2].includes(edgeWorkers)) throw new Error('edgeWorkers must be 1 or 2')
  if (
    !Number.isSafeInteger(retention.ms) ||
    retention.ms < 60000 ||
    !Number.isSafeInteger(retention.bytes) ||
    retention.bytes < 16777216
  )
    throw new Error('invalid event retention')
  const annotations = s.gatewayServiceAnnotations ?? {}
  if (
    typeof annotations !== 'object' ||
    Array.isArray(annotations) ||
    Object.entries(annotations).some(
      ([key, value]) => !/^([a-z0-9.-]+\/)?[A-Za-z0-9][-_.A-Za-z0-9]*$/.test(key) || typeof value !== 'string'
    )
  )
    throw new Error('invalid gateway Service annotations')
  const names = s.gatewayNames ?? []
  if (
    !Array.isArray(names) ||
    names.some(
      n =>
        typeof n !== 'string' ||
        n.length > 253 ||
        !/^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)+$/.test(n)
    )
  )
    throw new Error('invalid gateway names')
  const objects = []
  const resource = (apiVersion, kind, name, fields = {}) => ({
    apiVersion,
    kind,
    metadata: { name, namespace: s.namespace },
    ...fields
  })
  const add = (...args) => {
    const object = resource(...args)
    objects.push(object)
    return object
  }
  objects.push({
    apiVersion: 'v1',
    kind: 'Namespace',
    metadata: {
      name: s.namespace,
      labels: {
        'pod-security.kubernetes.io/enforce': 'restricted',
        'pod-security.kubernetes.io/enforce-version': 'v1.34'
      }
    }
  })
  const labels = component => ({ 'app.kubernetes.io/part-of': 'wts-ha', 'app.kubernetes.io/component': component })
  const selector = component => ({ matchLabels: labels(component) })
  const securityContext = {
    allowPrivilegeEscalation: false,
    readOnlyRootFilesystem: true,
    capabilities: { drop: ['ALL'] }
  }
  const podSecurity = {
    runAsNonRoot: true,
    runAsUser: 10001,
    runAsGroup: 10001,
    fsGroup: 10001,
    fsGroupChangePolicy: 'OnRootMismatch',
    seccompProfile: { type: 'RuntimeDefault' }
  }
  const secret = (name, secretName) => ({ name, secret: { secretName, defaultMode: 0o440 } })
  const mount = (name, mountPath) => ({ name, mountPath, readOnly: true })
  const anti = component => ({
    podAntiAffinity: {
      requiredDuringSchedulingIgnoredDuringExecution: [
        { labelSelector: selector(component), topologyKey: 'kubernetes.io/hostname' }
      ]
    }
  })
  const probe = (mode, period = 2) => ({
    exec: { command: ['container-probe', mode] },
    periodSeconds: period,
    timeoutSeconds: 1,
    failureThreshold: 1
  })
  const configName = `wts-source-${s.version}`
  add('v1', 'ServiceAccount', 'wts-gateway', { automountServiceAccountToken: false })
  add('v1', 'ServiceAccount', 'wts-feed', { automountServiceAccountToken: false })
  add('v1', 'ServiceAccount', 'wts-rollout', { automountServiceAccountToken: false })
  add('coordination.k8s.io/v1', 'Lease', 'wts-feed', { spec: { leaseDurationSeconds: 10 } })
  add('rbac.authorization.k8s.io/v1', 'Role', 'wts-feed', {
    rules: [
      {
        apiGroups: ['coordination.k8s.io'],
        resources: ['leases'],
        resourceNames: ['wts-feed'],
        verbs: ['get', 'update']
      }
    ]
  })
  add('rbac.authorization.k8s.io/v1', 'RoleBinding', 'wts-feed', {
    roleRef: { apiGroup: 'rbac.authorization.k8s.io', kind: 'Role', name: 'wts-feed' },
    subjects: [{ kind: 'ServiceAccount', name: 'wts-feed', namespace: s.namespace }]
  })
  add('rbac.authorization.k8s.io/v1', 'Role', 'wts-rollout', {
    rules: [
      {
        apiGroups: ['apps'],
        resources: ['deployments'],
        resourceNames: ['wts-gateway-a', 'wts-gateway-b', 'wts-gateway-c'],
        verbs: ['get', 'patch']
      },
      { apiGroups: [''], resources: ['pods'], verbs: ['get', 'list'] },
      {
        apiGroups: [''],
        resources: ['configmaps'],
        resourceNames: ['wts-rollout-lock', 'wts-rollout-state'],
        verbs: ['get', 'update', 'delete']
      },
      { apiGroups: [''], resources: ['configmaps'], verbs: ['create'] }
    ]
  })
  add('rbac.authorization.k8s.io/v1', 'RoleBinding', 'wts-rollout', {
    roleRef: { apiGroup: 'rbac.authorization.k8s.io', kind: 'Role', name: 'wts-rollout' },
    subjects: [{ kind: 'ServiceAccount', name: 'wts-rollout', namespace: s.namespace }]
  })
  add('v1', 'ConfigMap', configName, {
    immutable: true,
    data: {
      'source.json': JSON.stringify({
        log_id: 'ha-v1',
        source_id: { provider: 'synthetic', stream: 'trades' },
        epoch: 'epoch-1',
        start_seq: '1',
        topics: ['/topic/v1/kr/stock/trade/A005930', '/topic/v1/us/stock/trade/US19990122001']
      }),
      'lease.json': JSON.stringify({
        url: `https://kubernetes.default.svc/apis/coordination.k8s.io/v1/namespaces/${s.namespace}/leases/wts-feed`,
        token_file: '/var/run/lease/token',
        ca_file: '/var/run/lease/ca.crt',
        identity: 'feed',
        duration_seconds: 10
      })
    }
  })
  if (s.metricsNamespace)
    objects.find(o => o.kind === 'ConfigMap' && o.metadata.name === configName).data['metrics-proxy.js'] = readFileSync(
      new URL('./metrics-proxy.js', import.meta.url),
      'utf8'
    )
  for (const [index, slot] of ['a', 'b', 'c'].entries()) {
    const label = { ...labels('gateway'), 'wts-slot': slot }
    const edge = {
      name: 'edge',
      image: s.edgeImage,
      imagePullPolicy: 'IfNotPresent',
      restartPolicy: 'Always',
      securityContext,
      env: Object.entries({
        WTS_BACKEND: 'ws://127.0.0.1:8080/experimental/ha/ws',
        WTS_EDGE_HOST: '0.0.0.0',
        WTS_EDGE_PORT: '8443',
        WTS_TLS_CERT_FILE: '/tls/cert.pem',
        WTS_TLS_KEY_FILE: '/tls/key.pem',
        WTS_COMPRESSION: 'none',
        WTS_EDGE_WORKERS: String(edgeWorkers),
        NODE_OPTIONS: '--max-old-space-size=128'
      }).map(([name, value]) => ({ name, value })),
      ports: [{ name: 'wss', containerPort: 8443 }],
      volumeMounts: [mount('tls', '/tls')],
      resources: {
        requests: { cpu: '250m', memory: '128Mi' },
        limits: { cpu: '2', memory: edgeWorkers === 2 ? '1Gi' : '512Mi' }
      },
      startupProbe: { tcpSocket: { port: 'wss' }, periodSeconds: 2, timeoutSeconds: 1, failureThreshold: 30 },
      readinessProbe: { tcpSocket: { port: 'wss' }, periodSeconds: 2, timeoutSeconds: 1 },
      livenessProbe: { tcpSocket: { port: 'wss' }, periodSeconds: 10, timeoutSeconds: 1 }
    }
    const gateway = {
      name: 'gateway',
      image: s.gatewayImage,
      imagePullPolicy: 'IfNotPresent',
      securityContext,
      args: [
        'gateway',
        '--addr',
        '127.0.0.1:8080',
        '--http-addr',
        '127.0.0.1:8081',
        '--ha-kafka',
        '/credentials/gateway.json',
        '--ha-source',
        '/source/source.json',
        '--ha-cursor-keys',
        '/credentials/cursor-keys.json',
        '--ha-policy',
        '/credentials/policy-client.json',
        '--max-connections',
        '10000'
      ],
      volumeMounts: [mount('credentials', '/credentials'), mount('source', '/source')],
      resources: { requests: { cpu: '500m', memory: '256Mi' }, limits: { cpu: '2', memory: '1Gi' } },
      startupProbe: { ...probe('ha-live'), failureThreshold: 30 },
      readinessProbe: probe('ha-ready'),
      livenessProbe: { ...probe('ha-live', 10), failureThreshold: 3 },
      lifecycle: { preStop: { exec: { command: ['sh', '-c', 'container-probe ha-drain; sleep 20'] } } }
    }
    add('apps/v1', 'Deployment', `wts-gateway-${slot}`, {
      spec: {
        replicas: 1,
        minReadySeconds: 15,
        progressDeadlineSeconds: 600,
        revisionHistoryLimit: 3,
        strategy: { type: 'RollingUpdate', rollingUpdate: { maxUnavailable: 0, maxSurge: 1 } },
        selector: { matchLabels: label },
        template: {
          metadata: { labels: label, annotations: { 'wts/config-version': s.version } },
          spec: {
            serviceAccountName: 'wts-gateway',
            automountServiceAccountToken: false,
            terminationGracePeriodSeconds: 60,
            securityContext: podSecurity,
            nodeSelector: { 'topology.kubernetes.io/zone': s.zones[index] },
            affinity: anti('gateway'),
            initContainers: [
              edge,
              ...(s.metricsNamespace
                ? [
                    {
                      name: 'metrics',
                      image: s.edgeImage,
                      restartPolicy: 'Always',
                      command: ['node', '/source/metrics-proxy.js'],
                      securityContext,
                      ports: [{ name: 'metrics', containerPort: 9090 }],
                      volumeMounts: [mount('source', '/source')],
                      resources: { requests: { cpu: '25m', memory: '32Mi' }, limits: { cpu: '100m', memory: '64Mi' } },
                      startupProbe: { tcpSocket: { port: 'metrics' }, periodSeconds: 2, failureThreshold: 15 }
                    }
                  ]
                : [])
            ],
            containers: [gateway],
            volumes: [
              secret('credentials', `wts-gateway-${s.version}`),
              secret('tls', `wts-tls-${s.version}`),
              { name: 'source', configMap: { name: configName } }
            ]
          }
        }
      }
    })
  }
  if (s.metricsNamespace) {
    add('v1', 'Service', 'wts-ha-metrics', {
      spec: { selector: labels('gateway'), ports: [{ name: 'metrics', port: 9090, targetPort: 'metrics' }] }
    })
    add('networking.k8s.io/v1', 'NetworkPolicy', 'ha-metrics', {
      spec: {
        podSelector: selector('gateway'),
        policyTypes: ['Ingress'],
        ingress: [
          {
            from: [{ namespaceSelector: { matchLabels: { 'kubernetes.io/metadata.name': s.metricsNamespace } } }],
            ports: [{ protocol: 'TCP', port: 9090 }]
          }
        ]
      }
    })
  }
  add('policy/v1', 'PodDisruptionBudget', 'wts-gateway', { spec: { minAvailable: 2, selector: selector('gateway') } })
  const gateway = add('v1', 'Service', 'wts-gateway', {
    spec: {
      type: 'LoadBalancer',
      externalTrafficPolicy: 'Cluster',
      loadBalancerSourceRanges: s.clientCIDRs,
      selector: labels('gateway'),
      ports: [{ name: 'wss', port: 443, targetPort: 'wss' }]
    }
  })
  // AWS처럼 LB 종류와 target 방식을 Service annotation으로 받는 제공자를 위한 값이다.
  if (s.gatewayServiceAnnotations) gateway.metadata.annotations = { ...annotations }
  add('apps/v1', 'Deployment', 'wts-feed', {
    spec: {
      replicas: 2,
      strategy: { type: 'RollingUpdate', rollingUpdate: { maxUnavailable: 1, maxSurge: 0 } },
      selector: selector('feed'),
      template: {
        metadata: { labels: labels('feed') },
        spec: {
          serviceAccountName: 'wts-feed',
          automountServiceAccountToken: false,
          terminationGracePeriodSeconds: 30,
          securityContext: podSecurity,
          affinity: anti('feed'),
          topologySpreadConstraints: [
            {
              maxSkew: 1,
              topologyKey: 'topology.kubernetes.io/zone',
              whenUnsatisfiable: 'DoNotSchedule',
              labelSelector: selector('feed')
            }
          ],
          containers: [
            {
              name: 'feed',
              image: s.gatewayImage,
              securityContext,
              args: [
                'ha-feed',
                'run',
                '--kafka',
                '/credentials/feed.json',
                '--source',
                '/source/source.json',
                '--lease',
                '/source/lease.json'
              ],
              env: [{ name: 'POD_UID', valueFrom: { fieldRef: { fieldPath: 'metadata.uid' } } }],
              volumeMounts: [
                mount('credentials', '/credentials'),
                mount('source', '/source'),
                mount('lease', '/var/run/lease')
              ],
              resources: { requests: { cpu: '100m', memory: '128Mi' }, limits: { cpu: '1', memory: '512Mi' } }
            }
          ],
          volumes: [
            secret('credentials', `wts-feed-${s.version}`),
            { name: 'source', configMap: { name: configName } },
            {
              name: 'lease',
              projected: {
                defaultMode: 0o440,
                sources: [
                  { serviceAccountToken: { path: 'token', expirationSeconds: 3600 } },
                  { configMap: { name: 'kube-root-ca.crt', items: [{ key: 'ca.crt', path: 'ca.crt' }] } }
                ]
              }
            }
          ]
        }
      }
    }
  })
  add('policy/v1', 'PodDisruptionBudget', 'wts-feed', { spec: { minAvailable: 1, selector: selector('feed') } })
  add('v1', 'Service', 'wts-broker', {
    spec: {
      clusterIP: 'None',
      publishNotReadyAddresses: true,
      selector: labels('broker'),
      ports: [
        { name: 'kafka', port: 9092 },
        { name: 'controller', port: 9093 }
      ]
    }
  })
  add('apps/v1', 'StatefulSet', 'wts-broker', {
    spec: {
      serviceName: 'wts-broker',
      replicas: 3,
      podManagementPolicy: 'Parallel',
      updateStrategy: { type: 'OnDelete' },
      persistentVolumeClaimRetentionPolicy: { whenDeleted: 'Retain', whenScaled: 'Retain' },
      selector: selector('broker'),
      template: {
        metadata: { labels: labels('broker') },
        spec: {
          automountServiceAccountToken: false,
          terminationGracePeriodSeconds: 60,
          securityContext: { ...podSecurity, runAsUser: 1000, runAsGroup: 1000, fsGroup: 1000 },
          affinity: anti('broker'),
          topologySpreadConstraints: [
            {
              maxSkew: 1,
              minDomains: 3,
              topologyKey: 'topology.kubernetes.io/zone',
              whenUnsatisfiable: 'DoNotSchedule',
              labelSelector: selector('broker')
            }
          ],
          containers: [
            {
              name: 'broker',
              image: brokerImage,
              securityContext,
              command: ['/bin/bash', '-ec'],
              args: [
                'ordinal=${HOSTNAME##*-}; read -r cluster_id < /config/cluster-id; /opt/kafka/bin/kafka-storage.sh format --ignore-formatted --cluster-id="$cluster_id" -c "/config/broker$ordinal.properties"; exec /opt/kafka/bin/kafka-server-start.sh "/config/broker$ordinal.properties"'
              ],
              env: [{ name: 'KAFKA_HEAP_OPTS', value: '-Xms384m -Xmx512m' }],
              ports: [
                { name: 'kafka', containerPort: 9092 },
                { name: 'controller', containerPort: 9093 }
              ],
              volumeMounts: [
                mount('config', '/config'),
                { name: 'data', mountPath: '/var/lib/kafka/data' },
                { name: 'tmp', mountPath: '/tmp' },
                { name: 'logs', mountPath: '/opt/kafka/logs' }
              ],
              resources: { requests: { cpu: '500m', memory: '768Mi' }, limits: { cpu: '2', memory: '1536Mi' } },
              startupProbe: { tcpSocket: { port: 'kafka' }, periodSeconds: 5, failureThreshold: 60 },
              readinessProbe: {
                exec: {
                  command: [
                    '/bin/bash',
                    '-ec',
                    '/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 --command-config /config/health.properties >/dev/null 2>&1'
                  ]
                },
                periodSeconds: 10,
                timeoutSeconds: 8,
                failureThreshold: 2
              }
            }
          ],
          volumes: [
            secret('config', `wts-broker-${s.version}`),
            { name: 'tmp', emptyDir: { sizeLimit: '64Mi' } },
            { name: 'logs', emptyDir: { sizeLimit: '64Mi' } }
          ]
        }
      },
      volumeClaimTemplates: [
        {
          metadata: { name: 'data' },
          spec: {
            accessModes: ['ReadWriteOnce'],
            storageClassName: s.storageClass,
            resources: { requests: { storage: s.storageSize } }
          }
        }
      ]
    }
  })
  add('policy/v1', 'PodDisruptionBudget', 'wts-broker', { spec: { minAvailable: 2, selector: selector('broker') } })
  add('networking.k8s.io/v1', 'NetworkPolicy', 'default-deny', {
    spec: { podSelector: {}, policyTypes: ['Ingress', 'Egress'] }
  })
  const dns = {
    to: [
      {
        namespaceSelector: { matchLabels: { 'kubernetes.io/metadata.name': 'kube-system' } },
        podSelector: { matchLabels: { 'k8s-app': 'kube-dns' } }
      }
    ],
    ports: [
      { protocol: 'UDP', port: 53 },
      { protocol: 'TCP', port: 53 }
    ]
  }
  const peers = component => ({ podSelector: selector(component) })
  const ports = (...numbers) => numbers.map(port => ({ protocol: 'TCP', port }))
  const cidrs = values => values.map(cidr => ({ ipBlock: { cidr } }))
  add('networking.k8s.io/v1', 'NetworkPolicy', 'gateway', {
    spec: {
      podSelector: selector('gateway'),
      policyTypes: ['Ingress', 'Egress'],
      ingress: [{ from: cidrs(s.gatewayIngressCIDRs), ports: ports(8443) }],
      egress: [dns, { to: [peers('broker')], ports: ports(9092) }, { to: cidrs(s.policyCIDRs), ports: ports(443) }]
    }
  })
  add('networking.k8s.io/v1', 'NetworkPolicy', 'feed', {
    spec: {
      podSelector: selector('feed'),
      policyTypes: ['Ingress', 'Egress'],
      egress: [dns, { to: [peers('broker')], ports: ports(9092) }, { to: cidrs(s.apiCIDRs), ports: ports(443, 6443) }]
    }
  })
  add('networking.k8s.io/v1', 'NetworkPolicy', 'broker', {
    spec: {
      podSelector: selector('broker'),
      policyTypes: ['Ingress', 'Egress'],
      ingress: [
        { from: [peers('broker')], ports: ports(9092, 9093) },
        { from: [peers('feed'), peers('gateway'), peers('bootstrap')], ports: ports(9092) }
      ],
      egress: [dns, { to: [peers('broker')], ports: ports(9092, 9093) }]
    }
  })
  const bootstrap = `set -euo pipefail
admin=(--bootstrap-server wts-broker-0.wts-broker:9092 --command-config /admin/admin.properties)
topic() { /opt/kafka/bin/kafka-topics.sh "\${admin[@]}" "$@"; }
acl() { /opt/kafka/bin/kafka-acls.sh "\${admin[@]}" --add "$@" >/dev/null; }
topic --create --if-not-exists --topic wts.events.v1 --partitions 1 --replication-factor 3 --config min.insync.replicas=2 --config cleanup.policy=delete --config retention.ms=${retention.ms} --config retention.bytes=${retention.bytes} --config max.message.bytes=1048576
/opt/kafka/bin/kafka-configs.sh "\${admin[@]}" --alter --entity-type topics --entity-name wts.events.v1 --add-config retention.ms=${retention.ms},retention.bytes=${retention.bytes} >/dev/null
for name in wts.checkpoint.v1 wts.snapshot.v1; do topic --create --if-not-exists --topic "$name" --partitions 1 --replication-factor 3 --config min.insync.replicas=2 --config cleanup.policy=compact --config max.message.bytes=1048576 --config segment.bytes=16777216 --config segment.ms=60000 --config min.cleanable.dirty.ratio=0.1 --config max.compaction.lag.ms=120000; done
for name in wts.events.v1 wts.checkpoint.v1 wts.snapshot.v1; do acl --allow-principal User:feed --topic "$name" --operation Write --operation Read --operation Describe; done
for name in wts.events.v1 wts.snapshot.v1; do acl --allow-principal User:gateway --topic "$name" --operation Read --operation Describe; done
acl --allow-principal User:feed --transactional-id wts- --resource-pattern-type prefixed --operation Write --operation Describe
acl --allow-principal User:feed --allow-principal User:gateway --group wts-reader- --resource-pattern-type prefixed --operation Read
`
  add('batch/v1', 'Job', `wts-bootstrap-${s.version}`, {
    spec: {
      backoffLimit: 0,
      activeDeadlineSeconds: 300,
      template: {
        metadata: { labels: labels('bootstrap') },
        spec: {
          restartPolicy: 'Never',
          automountServiceAccountToken: false,
          securityContext: podSecurity,
          initContainers: [
            {
              name: 'topics',
              image: brokerImage,
              securityContext,
              command: ['/bin/bash', '-ec', bootstrap],
              volumeMounts: [
                mount('admin', '/admin'),
                { name: 'tmp', mountPath: '/tmp' },
                { name: 'logs', mountPath: '/opt/kafka/logs' }
              ],
              resources: { requests: { cpu: '100m', memory: '256Mi' }, limits: { cpu: '1', memory: '512Mi' } }
            }
          ],
          containers: [
            {
              name: 'initialize',
              image: s.gatewayImage,
              securityContext,
              args: ['ha-feed', 'init', '--kafka', '/credentials/feed.json', '--source', '/source/source.json'],
              volumeMounts: [mount('credentials', '/credentials'), mount('source', '/source')],
              resources: { requests: { cpu: '100m', memory: '128Mi' }, limits: { cpu: '1', memory: '512Mi' } }
            }
          ],
          volumes: [
            secret('admin', `wts-admin-${s.version}`),
            secret('credentials', `wts-feed-${s.version}`),
            { name: 'source', configMap: { name: configName } },
            { name: 'tmp', emptyDir: { sizeLimit: '64Mi' } },
            { name: 'logs', emptyDir: { sizeLimit: '64Mi' } }
          ]
        }
      }
    }
  })
  add('networking.k8s.io/v1', 'NetworkPolicy', 'bootstrap', {
    spec: {
      podSelector: selector('bootstrap'),
      policyTypes: ['Ingress', 'Egress'],
      egress: [dns, { to: [peers('broker')], ports: ports(9092) }]
    }
  })
  return { apiVersion: 'v1', kind: 'List', items: clone(objects) }
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    const list = render(JSON.parse(readFileSync(process.argv[2])))
    const stage = process.argv[3] ?? 'preview'
    if (!['preview', 'foundation', 'bootstrap', 'workloads'].includes(stage)) throw new Error('unknown render stage')
    if (stage === 'foundation') list.items = list.items.filter(o => o.kind !== 'Deployment' && o.kind !== 'Job')
    if (stage === 'bootstrap') list.items = list.items.filter(o => o.kind === 'Job')
    if (stage === 'workloads') list.items = list.items.filter(o => o.kind === 'Deployment')
    process.stdout.write(`${JSON.stringify(list, null, 2)}\n`)
  } catch (error) {
    console.error(error.message)
    process.exitCode = 1
  }
}
