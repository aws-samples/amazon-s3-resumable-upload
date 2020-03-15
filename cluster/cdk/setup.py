import setuptools


with open("README.md") as fp:
    long_description = fp.read()


setuptools.setup(
    name="s3_upload_cluster",
    version="0.0.1",

    description="s3_upload_cluster",
    long_description=long_description,
    long_description_content_type="text/markdown",

    author="huangzb@amazon.com",

    package_dir={"": "cdk"},
    packages=setuptools.find_packages(where="cdk"),

    install_requires=[
        "aws-cdk.core",
        "aws-cdk.aws-dynamodb",
        "aws-cdk.aws-events",
        "aws-cdk.aws-events-targets",
        "aws-cdk.aws-lambda",
        "aws-cdk.aws-s3",
        "aws-cdk.aws-ec2",
        "aws-cdk.aws-apigateway",
        "aws-cdk.aws-cloudwatch",
        "aws-cdk.aws-sqs",
        "aws-cdk.aws-iam"
        "cdk-watchful",
        "boto3"
    ],

    python_requires=">=3.6",

    classifiers=[
        "Development Status :: 4 - Beta",

        "Intended Audience :: Developers",

        "License :: OSI Approved :: Apache Software License",

        "Programming Language :: JavaScript",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",

        "Topic :: Software Development :: Code Generators",
        "Topic :: Utilities",

        "Typing :: Typed",
    ],
)
