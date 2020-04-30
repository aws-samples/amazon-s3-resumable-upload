
# Add to start application
echo "nohup python3 /home/ec2-user/amazon-s3-resumable-upload/s3_migration_cluster_worker.py &" >> /etc/rc.local
chmod +x /etc/rc.local

echo "Start python3 s3_migration_cluster_worker.py"
nohup python3 s3_migration_cluster_worker.py &
