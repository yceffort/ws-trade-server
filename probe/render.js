import { pathToFileURL } from 'node:url'
import { readBounded } from '../examples/ha-state.js'
import { validate } from './run.js'

export function render({ namespace, image, credentialsSecret, sourceConfigMap, config }) {
  if (![namespace, credentialsSecret, sourceConfigMap].every(value => typeof value === 'string' && /^[a-z0-9]([-a-z0-9]*[a-z0-9])?$/.test(value) && value.length <= 63) || !/^[\w./:-]+@sha256:[a-f0-9]{64}$/.test(image)) throw new Error('invalid deployment config')
  if (!Array.isArray(config.targets) || !config.targets.length || config.targets.length > 8) throw new Error('invalid targets')
  config.targets.forEach(validate)
  const labels = { 'app.kubernetes.io/part-of': 'wts-ha', 'app.kubernetes.io/component': 'probe' }
  const item = (kind, name, fields, apiVersion = 'v1') => ({ apiVersion, kind, metadata: { name, namespace }, ...fields })
  return { apiVersion: 'v1', kind: 'List', items: [
    item('ConfigMap', 'wts-probe', { data: { 'probe.json': JSON.stringify({ ...config, host: '0.0.0.0', port: 9464 }) } }),
    item('Deployment', 'wts-probe', { spec: { replicas: 1, selector: { matchLabels: labels }, template: { metadata: { labels }, spec: {
      automountServiceAccountToken: false, securityContext: { runAsNonRoot: true, runAsUser: 10001, runAsGroup: 10001, fsGroup: 10001, seccompProfile: { type: 'RuntimeDefault' } },
      containers: [{ name: 'probe', image, ports: [{ name: 'metrics', containerPort: 9464 }], readinessProbe: { httpGet: { path: '/metrics', port: 'metrics' }, periodSeconds: 5 }, resources: { requests: { cpu: '100m', memory: '128Mi' }, limits: { cpu: '1', memory: '1Gi' } }, securityContext: { allowPrivilegeEscalation: false, readOnlyRootFilesystem: true, capabilities: { drop: ['ALL'] } }, volumeMounts: [{ name: 'config', mountPath: '/config', readOnly: true }, { name: 'credentials', mountPath: '/credentials', readOnly: true }, { name: 'source', mountPath: '/source', readOnly: true }] }],
      volumes: [{ name: 'config', configMap: { name: 'wts-probe' } }, { name: 'credentials', secret: { secretName: credentialsSecret, defaultMode: 0o440 } }, { name: 'source', configMap: { name: sourceConfigMap } }],
    } } } }, 'apps/v1'),
    item('Service', 'wts-probe', { spec: { selector: labels, ports: [{ name: 'metrics', port: 9464, targetPort: 'metrics' }] } }),
    item('NetworkPolicy', 'wts-probe-metrics', { spec: { podSelector: { matchLabels: labels }, policyTypes: ['Ingress'], ingress: [{ from: [{ namespaceSelector: { matchLabels: { 'kubernetes.io/metadata.name': 'wts-observability' } } }], ports: [{ protocol: 'TCP', port: 9464 }] }] } }, 'networking.k8s.io/v1'),
    ...['gateway', 'broker'].map(component => item('NetworkPolicy', `wts-probe-${component}`, { spec: { podSelector: { matchLabels: { 'app.kubernetes.io/part-of': 'wts-ha', 'app.kubernetes.io/component': component } }, policyTypes: ['Ingress'], ingress: [{ from: [{ podSelector: { matchLabels: labels } }], ports: [{ protocol: 'TCP', port: component === 'gateway' ? 8443 : 9092 }] }] } }, 'networking.k8s.io/v1')),
  ] }
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try { console.log(JSON.stringify(render(JSON.parse(readBounded(process.argv[2], 64 * 1024))), null, 2)) }
  catch { console.error('invalid probe deployment config'); process.exitCode = 1 }
}
