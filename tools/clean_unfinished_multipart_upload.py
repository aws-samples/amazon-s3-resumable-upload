import boto3, json


# Get unfinished multipart upload id from s3
def get_uploaded_list(s3_client, Des_bucket, Des_key, MaxRetry):
    NextKeyMarker = ''
    IsTruncated = True
    multipart_uploaded_list = []
    while IsTruncated:
        IsTruncated = False
        for retry in range(MaxRetry + 1):
            try:
                print(f'Getting unfinished upload id list {retry} retry {Des_bucket}/{Des_key}...')
                list_multipart_uploads = s3_client.list_multipart_uploads(
                    Bucket=Des_bucket,
                    Prefix=Des_key,
                    MaxUploads=1000,
                    KeyMarker=NextKeyMarker
                )
                IsTruncated = list_multipart_uploads["IsTruncated"]
                NextKeyMarker = list_multipart_uploads["NextKeyMarker"]
                if "Uploads" in list_multipart_uploads:
                    for i in list_multipart_uploads["Uploads"]:
                        if i["Key"] == Des_key:
                            multipart_uploaded_list.append({
                                "Key": i["Key"],
                                "Initiated": i["Initiated"],
                                "UploadId": i["UploadId"]
                            })
                            print(f'Unfinished upload, Key: {i["Key"]}, Time: {i["Initiated"]}')
                break  # 退出重试循环
            except Exception as e:
                print(f'Fail to list multipart upload {str(e)}')
                if retry >= MaxRetry:
                    print(f'Fail MaxRetry list multipart upload {str(e)}')
                    return []
                else:
                    time.sleep(5 * retry)

    return multipart_uploaded_list


# Main
if __name__ == '__main__':

    session = boto3.session.Session(profile_name='zhy')
    s3_des_client = session.client('s3')
    Des_bucket = 's3-migration-test-nx'

    multipart_uploaded_list = get_uploaded_list(s3_des_client, Des_bucket, "", 3)
    for clean_i in multipart_uploaded_list:
        try:
            s3_des_client.abort_multipart_upload(
                Bucket=Des_bucket,
                Key=clean_i["Key"],
                UploadId=clean_i["UploadId"]
            )
        except Exception as e:
            print(f'Fail to clean {str(e)}')
    multipart_uploaded_list = []
    print('CLEAN UNFINISHED UPLOAD FINISHED: ', Des_bucket)

