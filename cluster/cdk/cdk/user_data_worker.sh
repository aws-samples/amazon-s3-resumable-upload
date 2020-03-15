#!/bin/bash
echo "yum update -y"
yum update -y

echo "Install git, python3 and boto3"
yum install git -y
yum install python3 -y
pip3 install boto3

echo "Download application amazon-s3-resumable-upload.git"
cd /home/ec2-user/  || exit
git clone -b master https://github.com/aws-samples/amazon-s3-resumable-upload.git
chown -R ec2-user:ec2-user amazon-s3-resumable-upload/

# Setup BBR
echo "Setup BBR"
/bin/cp -rf /home/ec2-user/amazon-s3-resumable-upload/cluster/tcpcong.modules /etc/sysconfig/modules/tcpcong.modules
chmod 755 /etc/sysconfig/modules/tcpcong.modules
echo "net.ipv4.tcp_congestion_control = bbr" >> /etc/sysctl.d/00-tcpcong.conf
modprobe tcp_bbr
modprobe sch_fq
sysctl -w net.ipv4.tcp_congestion_control=bbr

# Add to start application
echo "nohup python3 /home/ec2-user/amazon-s3-resumable-upload/cluster/s3_migration_cluster_worker.py &" >> /etc/rc.local

echo "Start python3 s3_migration_cluster_worker.py"
cd amazon-s3-resumable-upload/cluster/
nohup python3 s3_migration_cluster_worker.py &
