## DynamoDB scan tool
### Setup
Change these two lines as your wish:
```
table_queue_name = "s3_migrate_file_list"
src_session = boto3.session.Session(profile_name='iad')
```
### What has it done?
* Scan and save the whole DynamoDB talbe into a csv file in local.
* Show the running jobs. You can change limit, default 10.
* Show the descend order of recent start jobs. You can change limit, default 10.