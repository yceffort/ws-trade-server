#!/bin/bash
# 임시 클러스터를 만들고 측정에 필요한 것(자동 축소 예약, NLB 전용 서브넷, StorageClass, LB 컨트롤러)을 준비한다.
# 사용: deploy/aws/up.sh [노드를 0대로 줄일 때까지의 시간, 기본 8]
set -euo pipefail
cd "$(dirname "$0")"
export AWS_DEFAULT_REGION=ap-northeast-2 AWS_PAGER=""
CLUSTER=wts-ha-aws
# 기존 kubeconfig와 현재 context를 건드리지 않도록 전용 파일에만 쓴다.
export KUBECONFIG="$PWD/../local/aws/kubeconfig"
mkdir -p "$(dirname "$KUBECONFIG")"

eksctl get cluster --name "$CLUSTER" >/dev/null 2>&1 || eksctl create cluster -f cluster.yaml --kubeconfig "$KUBECONFIG"

# 세션이 끊겨도 노드 요금이 계속 나가지 않게, 정한 시각에 모든 노드 그룹을 0대로 줄인다.
AT=$(python3 -c "import datetime,sys;print((datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(hours=float(sys.argv[1]))).strftime('%Y-%m-%dT%H:%M:%SZ'))" "${1:-8}")
for GROUP in $(aws autoscaling describe-auto-scaling-groups --filters "Name=tag:eks:cluster-name,Values=$CLUSTER" --query 'AutoScalingGroups[].AutoScalingGroupName' --output text); do
  aws autoscaling put-scheduled-update-group-action --auto-scaling-group-name "$GROUP" --scheduled-action-name wts-stop --start-time "$AT" --min-size 0 --max-size 0 --desired-capacity 0
done
echo "노드 자동 축소 예약: $AT"

# NLB를 Pod와 다른 서브넷에 둬야 gateway의 NetworkPolicy가 NLB에서 온 접속만 허용할 수 있다.
VPC=$(aws eks describe-cluster --name "$CLUSTER" --query cluster.resourcesVpcConfig.vpcId --output text)
INDEX=0
for ZONE in a b c; do
  FOUND=$(aws ec2 describe-subnets --filters "Name=tag:Name,Values=$CLUSTER-nlb-$ZONE" --query 'Subnets[0].SubnetId' --output text)
  [ "$FOUND" != None ] || aws ec2 create-subnet --vpc-id "$VPC" --availability-zone "ap-northeast-2$ZONE" --cidr-block "192.168.192.$((INDEX * 16))/28" \
    --tag-specifications "ResourceType=subnet,Tags=[{Key=Name,Value=$CLUSTER-nlb-$ZONE},{Key=project,Value=ws-trade-server}]" --query Subnet.SubnetId --output text
  INDEX=$((INDEX + 1))
done

kubectl apply -f - <<'EOF'
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata: { name: wts-gp3 }
provisioner: ebs.csi.aws.com
volumeBindingMode: WaitForFirstConsumer
reclaimPolicy: Delete
# ext4는 볼륨 최상위에 lost+found를 만들고, Kafka는 로그 디렉터리에 그런 항목이 있으면 시작하지 않는다(EKS에서 관측).
parameters: { type: gp3, csi.storage.k8s.io/fstype: xfs, tagSpecification_1: project=ws-trade-server }
EOF

helm repo add eks https://aws.github.io/eks-charts >/dev/null
helm upgrade --install aws-load-balancer-controller eks/aws-load-balancer-controller --namespace kube-system \
  --set clusterName="$CLUSTER" --set region="$AWS_DEFAULT_REGION" --set vpcId="$VPC" --set serviceAccount.name=aws-load-balancer-controller --wait
