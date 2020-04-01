#!/bin/bash -v
yum update -y
echo "Install git, python3 and boto3"
yum install git -y
yum install python3 -y
pip3 install boto3

echo "Download application amazon-s3-resumable-upload.git"
cd /home/ec2-user/  || exit
git clone -b master https://github.com/aws-samples/amazon-s3-resumable-upload.git
chown -R ec2-user:ec2-user amazon-s3-resumable-upload/

# Install CW Agent
echo "Install CW Agent"
wget https://s3.amazonaws.com/amazoncloudwatch-agent/amazon_linux/amd64/latest/amazon-cloudwatch-agent.rpm
rpm -U ./amazon-cloudwatch-agent.rpm
/opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl -a fetch-config -m ec2 -c file:/home/ec2-user/amazon-s3-resumable-upload/cluster/cw-agent-config.json -s

# Setup BBR
echo "Setup BBR"
/bin/cp -rf /home/ec2-user/amazon-s3-resumable-upload/cluster/tcpcong.modules /etc/sysconfig/modules/tcpcong.modules
chmod 755 /etc/sysconfig/modules/tcpcong.modules
echo "net.ipv4.tcp_congestion_control = bbr" >> /etc/sysctl.d/00-tcpcong.conf
modprobe tcp_bbr
modprobe sch_fq
sysctl -w net.ipv4.tcp_congestion_control=bbr

# Add to start application
echo "nohup python3 /home/ec2-user/amazon-s3-resumable-upload/cluster/s3_migration_cluster_jobsender.py &" >> /etc/rc.local

echo "Start python3 s3_migration_cluster_jobsender.py"
cd amazon-s3-resumable-upload/cluster/  || exit
nohup python3 s3_migration_cluster_jobsender.py &
