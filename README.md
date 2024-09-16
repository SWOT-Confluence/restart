# restart

Restarts Confluence workflow after failure by:

1) Locating failures.
2) Removing failed identifiers from coordinating JSON files located on EFS.
3) Saves failures as JSON data for review.
4) Uploads JSON data to appropriate S3 buckets to save data and start a new execution of the workflow.
5) Starts a new execution of the workflow.
6) Deletes all data saved from previous execution so errors can be handled again.


This is a docker container meant to be run as an AWS Batch job after any failures in Confluence workflow step function tasks.

## Arguments

- d: Input directory path
- p: Prefix for venue running in
- v: Version of Confluence (4-digit integer)
- r: Run type, 'constrained' or 'unconstrained'
- e: Indicate running on a subset
- s: Indicate restart execution
- f: Indicate remove failures from EFS files
- t: Tolerated failure percentage

You may run a subset of operations to produce and upload a report of failures by *not* including the `-s` and `-f` arguments.

## installation

Build a Docker image: `docker build -t restart .`

## execution

AWS credentials will need to be passed as environment variables to the container so that `restart` may access AWS infrastructure to generate JSON files.

```bash
# Credentials
export aws_key=XXXXXXXXXXXXXX
export aws_secret=XXXXXXXXXXXXXXXXXXXXXXXXXX

# Docker run command
docker run --rm --name restart -e AWS_ACCESS_KEY_ID=$aws_key -e AWS_SECRET_ACCESS_KEY=$aws_secret -e AWS_DEFAULT_REGION=us-west-2 restart:latest
```

## deployment

There is a script to deploy the Docker container image and Terraform AWS infrastructure found in the `deploy` directory.

Script to deploy Terraform and Docker image AWS infrastructure

REQUIRES:

- jq (<https://jqlang.github.io/jq/>)
- docker (<https://docs.docker.com/desktop/>) > version Docker 1.5
- AWS CLI (<https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html>)
- Terraform (<https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli>)

Command line arguments:

[1] registry: Registry URI
[2] repository: Name of repository to create
[3] prefix: Prefix to use for AWS resources associated with environment deploying to
[4] s3_state_bucket: Name of the S3 bucket to store Terraform state in (no need for s3:// prefix)
[5] profile: Name of profile used to authenticate AWS CLI commands

Example usage: ``./deploy.sh "account-id.dkr.ecr.region.amazonaws.com" "container-image-name" "prefix-for-environment" "s3-state-bucket-name" "confluence-named-profile"`

Note: Run the script from the deploy directory.
