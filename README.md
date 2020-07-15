# William Hill Futures Processor

Retrieve futures from s3. Ingestion is kicked off using Cloudwatch s3 event and persisted into documentdb. 

## Folder Structure

| Folder        | Purpose       |
| ------------- |:-------------:|
|/build         | Build related configuration|
|/scripts | Shell scripts|
|/src/handler | Lambda handlers functions|
|/tests/unit | Unit test specs|

## Getting started
Requirements:
- Serverless Framework
- AWS CLI
- Okta.jar login with assumed role scoring-developer

## Pipelines
Pipelines are set up in  in qa, and prod for automated deployments from github triggered by merge into master. They will be created by running:
```bash
aws cloudformation create-stack --stack-name dataeng-futures-wh-process --template-body file://build/pipeline-qa.yml --capabilities CAPABILITY_NAMED_IAM --parameters '[{"ParameterKey":"ProjectName","ParameterValue":"dataeng-futures-wh-process"}]'
```

## Deploy from local to dev
With your own personalized stackName (STAGE=usernameMMDD)
The default serverless deploy creates a stack named ${self:service}-${self:provider.stage}
```bash
# Create new stack in scoring-dev account named: dataeng-futures-wh-wh-gwyman1108
AWS_PROFILE=scoring-qa; STAGE=gwyman1108; serverless deploy --stage $STAGE --verbose --aws-s3-accelerate
```
