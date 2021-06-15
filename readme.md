# DynamoDB to Algolia

This is a sample project how to process changes in the aws dynamo table using streams that trigger a lambda function that's loaded as container, which updates an Algolia search index.

## Build and Deployment

Create a new "repository" (container name) in ECR (public or private registry). Push the newly build image there. Then create a lambda function from container image. You may also want to use a custom IAM role to run the function. Add lambda trigger, whatever dynamo table, one table per trigger. Enjoy!

```
docker build . -t ooo.dkr.ecr.rrr.amazonaws.com/nnn:latest
aws ecr get-login-password --region rrr | docker login --username AWS --password-stdin ooo.dkr.ecr.rrr.amazonaws.com
docker push ooo.dkr.ecr.rrr.amazonaws.com/nnn:latest
```

* `ooo` is your organisation id
* `rrr` is your region
* `nnn` is your chosen docker container name
