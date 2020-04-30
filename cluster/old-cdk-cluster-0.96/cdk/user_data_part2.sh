
EOF
/opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl -a fetch-config -m ec2 -c file:/root/cw-agent-config.json -s

echo "Download application amazon-s3-resumable-upload.git"
cd /home/ec2-user/  || exit
mkdir amazon-s3-resumable-upload/
chown -R ec2-user:ec2-user amazon-s3-resumable-upload/
cd amazon-s3-resumable-upload  || exit

aws s3 sync s3://