# S3ContentPush
This is a meven project for amazon s3 content copy

Setting up application:
1. provide required details like accesskey, secretKey, source bucket, destination bucket, source folder, destination folder, no. of thread and from date(if choose to do an incremental copy)

Build:
  mvn clean install
  
Run:
    1. Run AmazonS3FolderCopy as java application in eclipse OR
    2. Run 'java -cp <project location/target/s3contentpush-v1.0.jar com.s3content.push.AmazonS3FolderCopy' in command prompt
