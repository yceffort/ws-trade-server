#!/bin/bash
# AZ 하나의 손실을 주입한다. 요금이 붙는 FIS 대신 같은 원리를 직접 쓴다.
# 사용: deploy/aws/az-fault.sh isolate <a|b|c> <초>   그 AZ의 모든 서브넷을 전부 거부하는 NACL로 바꿨다가 되돌린다.
#       deploy/aws/az-fault.sh stop <a|b|c> <초>      그 AZ의 서버 인스턴스를 예고 없이 종료하고, 정한 시간 뒤 다시 2대로 늘린다.
set -uo pipefail
ACTION=$1 ZONE=$2 SECONDS_DOWN=$3
export AWS_DEFAULT_REGION=ap-northeast-2 AWS_PAGER=""
CLUSTER=wts-ha-aws
stamp() { echo "$(date -u +%H:%M:%S) $*"; }

if [ "$ACTION" = isolate ]; then
  VPC=$(aws eks describe-cluster --name "$CLUSTER" --query cluster.resourcesVpcConfig.vpcId --output text)
  DENY=$(aws ec2 create-network-acl --vpc-id "$VPC" --tag-specifications "ResourceType=network-acl,Tags=[{Key=Name,Value=$CLUSTER-az-fault},{Key=project,Value=ws-trade-server}]" --query NetworkAcl.NetworkAclId --output text)
  RESTORE=()
  restore() {
    for PAIR in ${RESTORE[@]+"${RESTORE[@]}"}; do
      aws ec2 replace-network-acl-association --association-id "${PAIR%%=*}" --network-acl-id "${PAIR##*=}" >/dev/null
    done
    aws ec2 delete-network-acl --network-acl-id "$DENY"
    stamp "restored"
  }
  # 새 NACL에는 규칙이 없어 들어오고 나가는 모든 패킷을 버린다. NACL은 상태가 없으므로 이미 맺은 연결도 끊긴다.
  trap restore EXIT
  for SUBNET in $(aws ec2 describe-subnets --filters "Name=vpc-id,Values=$VPC" "Name=availability-zone,Values=ap-northeast-2$ZONE" --query 'Subnets[].SubnetId' --output text); do
    read -r ASSOCIATION ORIGINAL <<<"$(aws ec2 describe-network-acls --filters "Name=association.subnet-id,Values=$SUBNET" --query "NetworkAcls[0].[Associations[?SubnetId=='$SUBNET']|[0].NetworkAclAssociationId,NetworkAclId]" --output text)"
    NEW=$(aws ec2 replace-network-acl-association --association-id "$ASSOCIATION" --network-acl-id "$DENY" --query NewAssociationId --output text)
    RESTORE+=("$NEW=$ORIGINAL")
  done
  stamp "isolated ap-northeast-2$ZONE (${#RESTORE[@]} subnets)"
  sleep "$SECONDS_DOWN"
else
  GROUP=$(aws autoscaling describe-auto-scaling-groups --filters "Name=tag:eks:nodegroup-name,Values=server-$ZONE" "Name=tag:eks:cluster-name,Values=$CLUSTER" --query 'AutoScalingGroups[0].AutoScalingGroupName' --output text)
  INSTANCES=$(aws autoscaling describe-auto-scaling-groups --auto-scaling-group-names "$GROUP" --query 'AutoScalingGroups[0].Instances[].InstanceId' --output text)
  # 대체 인스턴스가 바로 뜨지 않게 그룹 크기를 먼저 0으로 묶는다.
  aws autoscaling update-auto-scaling-group --auto-scaling-group-name "$GROUP" --min-size 0 --max-size 0 --desired-capacity 0
  # shellcheck disable=SC2086
  aws ec2 terminate-instances --instance-ids $INSTANCES >/dev/null
  stamp "terminated ap-northeast-2$ZONE servers"
  sleep "$SECONDS_DOWN"
  aws autoscaling update-auto-scaling-group --auto-scaling-group-name "$GROUP" --min-size 0 --max-size 2 --desired-capacity 2
  stamp "restored"
fi
