"""
AWS 리소스 조회 API
VPC, Subnet, Security Group 등을 조회하여 Source 생성 시 사용
"""
from typing import List, Optional
from fastapi import APIRouter, HTTPException, Query
from botocore.exceptions import ClientError
from utils.aws_client import get_aws_client

router = APIRouter()


@router.get("/vpcs")
async def list_vpcs():
    """
    VPC 목록 조회
    AWS Glue Connection 생성 시 VPC 선택 드롭다운에 사용
    """
    try:
        ec2 = get_aws_client('ec2')
        response = ec2.describe_vpcs()
        
        return {
            "vpcs": [
                {
                    "vpc_id": vpc["VpcId"],
                    "cidr_block": vpc["CidrBlock"],
                    "state": vpc["State"],
                    "is_default": vpc.get("IsDefault", False),
                    "tags": {tag["Key"]: tag["Value"] for tag in vpc.get("Tags", [])}
                }
                for vpc in response["Vpcs"]
            ]
        }
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"AWS API Error: {str(e)}")


@router.get("/subnets")
async def list_subnets(vpc_id: str = Query(..., description="VPC ID to filter subnets")):
    """
    특정 VPC의 Subnet 목록 조회
    VPC 선택 후 Subnet 드롭다운에 사용
    """
    try:
        ec2 = get_aws_client('ec2')
        response = ec2.describe_subnets(
            Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
        )
        
        return {
            "subnets": [
                {
                    "subnet_id": subnet["SubnetId"],
                    "cidr_block": subnet["CidrBlock"],
                    "availability_zone": subnet["AvailabilityZone"],
                    "availability_zone_id": subnet.get("AvailabilityZoneId"),
                    "available_ip_count": subnet["AvailableIpAddressCount"],
                    "state": subnet["State"],
                    "tags": {tag["Key"]: tag["Value"] for tag in subnet.get("Tags", [])}
                }
                for subnet in response["Subnets"]
            ]
        }
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"AWS API Error: {str(e)}")


@router.get("/security-groups")
async def list_security_groups(vpc_id: str = Query(..., description="VPC ID to filter security groups")):
    """
    특정 VPC의 Security Group 목록 조회
    VPC 선택 후 Security Group 선택에 사용
    """
    try:
        ec2 = get_aws_client('ec2')
        response = ec2.describe_security_groups(
            Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
        )
        
        return {
            "security_groups": [
                {
                    "group_id": sg["GroupId"],
                    "group_name": sg["GroupName"],
                    "description": sg["Description"],
                    "tags": {tag["Key"]: tag["Value"] for tag in sg.get("Tags", [])}
                }
                for sg in response["SecurityGroups"]
            ]
        }
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"AWS API Error: {str(e)}")


@router.get("/availability-zones")
async def list_availability_zones():
    """
    사용 가능한 Availability Zone 목록
    Subnet 생성 시 AZ 선택에 사용
    """
    try:
        ec2 = get_aws_client('ec2')
        response = ec2.describe_availability_zones(
            Filters=[{"Name": "state", "Values": ["available"]}]
        )
        
        return {
            "availability_zones": [
                {
                    "zone_name": az["ZoneName"],
                    "zone_id": az["ZoneId"],
                    "region_name": az["RegionName"],
                    "state": az["State"]
                }
                for az in response["AvailabilityZones"]
            ]
        }
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"AWS API Error: {str(e)}")
