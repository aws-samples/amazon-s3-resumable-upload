
EOF
/opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl -a fetch-config -m ec2 -c file:/root/cw-agent-config.json -s

echo "Download application amazon-s3-resumable-upload.git"
cd /home/ec2-user/  || exit
git clone -b master https://github.com/aws-samples/amazon-s3-resumable-upload.git
chown -R ec2-user:ec2-user amazon-s3-resumable-upload/

# Add to start application
echo "nohup python3 /home/ec2-user/amazon-s3-resumable-upload/cluster/s3_migration_cluster_worker.py &" >> /etc/rc.local
chmod +x /etc/rc.local

echo "Start python3 s3_migration_cluster_worker.py"
cd amazon-s3-resumable-upload/cluster/  || exit
nohup python3 s3_migration_cluster_worker.py &
