# emr_cli
A command line utility for spinning up a EMR clusters from config files.

## Requirements 
* Java 8

## Usage
* create cluster `java -jar emr_cli.jar create-cluster --conf mm_conf.yml`

## Configuration fields
* clusterName: the name of the cluster
* logUri: the s3 path you want to store logs in
* subnet: the subnet the cluster gets created in
* instanceType: duh
* pemKey: the name of the key used in the cluster. Do not inclued the `.pem` extension
* instanceCount: quantity of instances
* emrVersion: duh. defaults to 6.1.0
* bidPct: percent of max price that you want to pay
* jar: s3 path of the jar you want to run (optional)
* jarClass: the entrypoint class of your jar (optional)
* jarArgs: arguments for the jar
* serviceRole: the name of the role EMR service assumes to create the cluster (typically EMR_DefaultRole)
* instanceProfile: the ARN of the IAM profile that you want the cluster instances to use 
* callerRole: if you need to assume a role to create the cluster pass the ARN here (optional) 
* tags: any tags you want to apply

## Example configurations:

### Run a jar
```yaml
clusterName: mycluster
logUri: "s3://mys3bucket/logs/"
subnet: my-subnet-120938
instanceType: "c5.4xlarge"
pemKey: mypemkey
instanceCount: 15
bidPct: 50
serviceRole: EMR_DefaultRole
jar: "s3://path/to/jar/myjar.jar"
jarClass: com.warrenronsiek.myjar.Main
jarArgs:
  - "--env"
  - "test"
  - "--date"
  - "2020-10-28"
instanceProfile: instance-profile
callerRole: arn:aws:iam::1234567890:role/role-to-assume
tags:
  - Key: Application Type
    Value: fizzbuzz
```

### Analytics
```yaml
clusterName: mycluster
logUri: "s3://mys3bucket/logs/"
subnet: my-subnet-120938
instanceType: "c5.4xlarge"
pemKey: mypemkey
instanceCount: 15
bidPct: 50
serviceRole: EMR_DefaultRole
instanceProfile: instance-profile
callerRole: arn:aws:iam::1234567890:role/role-to-assume
tags:
  - Key: Application Type
    Value: fizzbuzz
```
