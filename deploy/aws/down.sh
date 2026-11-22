#!/bin/bash
# 측정이 끝난 클러스터와 딸린 자원을 지우고 남은 것이 없는지 확인한다. 남은 것이 있으면 0이 아닌 값으로 끝난다.
# 사용: deploy/aws/down.sh [check]  (check는 지우지 않고 남은 자원만 센다)
set -uo pipefail
cd "$(dirname "$0")"
export AWS_DEFAULT_REGION=ap-northeast-2 AWS_PAGER=""
CLUSTER=wts-ha-aws
export KUBECONFIG="$PWD/../local/aws/kubeconfig"
count() { "$@" --output text 2>/dev/null | wc -w | tr -d ' '; }

if [ "${1:-}" != check ]; then
  # NLB와 EBS 볼륨은 클러스터 안의 컨트롤러가 지우므로 클러스터보다 먼저 namespace를 지운다.
  kubectl delete namespace wts-ha --ignore-not-found --timeout=300s
  for _ in $(seq 60); do
    [ "$(count aws elbv2 describe-load-balancers --query 'LoadBalancers[].LoadBalancerArn')" = 0 ] && break
    sleep 5
  done
  for SUBNET in $(aws ec2 describe-subnets --filters "Name=tag:Name,Values=$CLUSTER-nlb-*" --query 'Subnets[].SubnetId' --output text); do
    aws ec2 delete-subnet --subnet-id "$SUBNET"
  done
  eksctl delete cluster --name "$CLUSTER" --wait --disable-nodegroup-eviction
  for REPOSITORY in gateway edge load; do
    aws ecr delete-repository --repository-name "wts-ha/$REPOSITORY" --force >/dev/null 2>&1
  done
fi

LEFT=0
report() { echo "$1: $2"; LEFT=$((LEFT + $2)); }
report "EKS 클러스터" "$(count aws eks list-clusters --query 'clusters')"
report "EC2 인스턴스" "$(count aws ec2 describe-instances --filters Name=instance-state-name,Values=pending,running,stopping,stopped --query 'Reservations[].Instances[].InstanceId')"
report "EBS 볼륨" "$(count aws ec2 describe-volumes --query 'Volumes[].VolumeId')"
report "로드 밸런서" "$(count aws elbv2 describe-load-balancers --query 'LoadBalancers[].LoadBalancerArn')"
report "target group" "$(count aws elbv2 describe-target-groups --query 'TargetGroups[].TargetGroupArn')"
report "NAT gateway" "$(count aws ec2 describe-nat-gateways --filter Name=state,Values=pending,available --query 'NatGateways[].NatGatewayId')"
report "Elastic IP" "$(count aws ec2 describe-addresses --query 'Addresses[].AllocationId')"
report "기본 VPC 밖의 ENI" "$(count aws ec2 describe-network-interfaces --filters "Name=tag:cluster.k8s.amazonaws.com/name,Values=$CLUSTER" --query 'NetworkInterfaces[].NetworkInterfaceId')"
report "eksctl VPC" "$(count aws ec2 describe-vpcs --filters "Name=tag:alpha.eksctl.io/cluster-name,Values=$CLUSTER" --query 'Vpcs[].VpcId')"
report "CloudFormation 스택" "$(count aws cloudformation list-stacks --stack-status-filter CREATE_COMPLETE CREATE_FAILED DELETE_FAILED DELETE_IN_PROGRESS UPDATE_COMPLETE ROLLBACK_COMPLETE --query "StackSummaries[?starts_with(StackName, 'eksctl-$CLUSTER')].StackName")"
report "ECR 저장소" "$(count aws ecr describe-repositories --query "repositories[?starts_with(repositoryName, 'wts-ha/')].repositoryName")"
[ "$LEFT" = 0 ] && echo "남은 자원 없음" || { echo "남은 자원 ${LEFT}건"; exit 1; }
