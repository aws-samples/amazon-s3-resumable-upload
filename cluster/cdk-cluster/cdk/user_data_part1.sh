#!/bin/bash -v
yum update -y
yum install git -y
yum install python3 -y
pip3 install boto3

# Setup BBR
echo "Setup BBR"
cat <<EOF>> /etc/sysconfig/modules/tcpcong.modules
#!/bin/bash
exec /sbin/modprobe tcp_bbr >/dev/null 2>&1
exec /sbin/modprobe sch_fq >/dev/null 2>&1
EOF
chmod 755 /etc/sysconfig/modules/tcpcong.modules
echo "net.ipv4.tcp_congestion_control = bbr" >> /etc/sysctl.d/00-tcpcong.conf
modprobe tcp_bbr
modprobe sch_fq
sysctl -w net.ipv4.tcp_congestion_control=bbr

# Install CW Agent
echo "Install CW Agent"
wget https://s3.amazonaws.com/amazoncloudwatch-agent/amazon_linux/amd64/latest/amazon-cloudwatch-agent.rpm
rpm -U ./amazon-cloudwatch-agent.rpm
# Create CWAgent config file
cat <<EOF>> /root/cw-agent-config.json
