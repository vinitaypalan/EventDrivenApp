# EventDrivenApp

A serverless order-processing application built with AWS SAM. Orders submitted through API Gateway are queued in Amazon SQS and processed asynchronously by AWS Lambda. DynamoDB transactions and idempotency records protect against duplicate delivery, while Amazon EventBridge and SNS provide event notifications.

## Architecture

### API order-processing flow

<p align="center">
  <img alt="Client" width="48" src="https://raw.githubusercontent.com/awslabs/aws-icons-for-plantuml/main/dist/General/Users.png" />
  &nbsp; Client &nbsp;→&nbsp;
  <img alt="Amazon API Gateway" width="48" src="https://raw.githubusercontent.com/awslabs/aws-icons-for-plantuml/main/dist/NetworkingContentDelivery/APIGateway.png" />
  &nbsp; API Gateway &nbsp;→&nbsp;
  <img alt="Amazon SQS" width="48" src="https://raw.githubusercontent.com/awslabs/aws-icons-for-plantuml/main/dist/ApplicationIntegration/SimpleQueueServiceQueue.png" />
  &nbsp; API ingress queue &nbsp;→&nbsp;
  <img alt="AWS Lambda" width="48" src="https://raw.githubusercontent.com/awslabs/aws-icons-for-plantuml/main/dist/Compute/Lambda.png" />
  &nbsp; ProcessOrderFunction &nbsp;→&nbsp;
  <img alt="Amazon DynamoDB" width="48" src="https://raw.githubusercontent.com/awslabs/aws-icons-for-plantuml/main/dist/Database/DynamoDB.png" />
  &nbsp; Orders + idempotency
</p>

<p align="center">
  <img alt="AWS Lambda" width="48" src="https://raw.githubusercontent.com/awslabs/aws-icons-for-plantuml/main/dist/Compute/Lambda.png" />
  &nbsp; ProcessOrderFunction &nbsp;→&nbsp;
  <img alt="Amazon EventBridge" width="48" src="https://raw.githubusercontent.com/awslabs/aws-icons-for-plantuml/main/dist/ApplicationIntegration/EventBridgeCustomEventBus.png" />
  &nbsp; OrdersEventBus &nbsp;→&nbsp;
  <img alt="Amazon SNS" width="48" src="https://raw.githubusercontent.com/awslabs/aws-icons-for-plantuml/main/dist/ApplicationIntegration/SimpleNotificationService.png" />
  &nbsp; Notifications
</p>

`POST /orders` is integrated directly with SQS `SendMessage`. The API returns **202 Accepted** when SQS accepts a valid request; this response does not mean the order has finished processing.

### Direct Lambda and EventBridge flow

The template also deploys `CreateOrderFunction`, a separate Lambda entry point that is **not wired to the API**. When invoked directly with an order, it writes to the existing Orders table and publishes an `OrderCreated` event. EventBridge rules route order events to the notification Lambda, the SNS topic, and (for `OrderCreated`) a separate processing queue. The processor currently acknowledges legacy `OrderCreated` events without writing the order again; the API/SQS flow above is the path that performs idempotent order processing.

<p align="center">
  <img alt="AWS Lambda" width="48" src="https://raw.githubusercontent.com/awslabs/aws-icons-for-plantuml/main/dist/Compute/Lambda.png" />
  &nbsp; CreateOrderFunction &nbsp;→&nbsp;
  <img alt="Amazon EventBridge" width="48" src="https://raw.githubusercontent.com/awslabs/aws-icons-for-plantuml/main/dist/ApplicationIntegration/EventBridgeCustomEventBus.png" />
  &nbsp; OrdersEventBus &nbsp;→&nbsp;
  <img alt="AWS Lambda" width="48" src="https://raw.githubusercontent.com/awslabs/aws-icons-for-plantuml/main/dist/Compute/Lambda.png" />
  &nbsp; OrderNotificationFunction &nbsp;→&nbsp;
  <img alt="Amazon SNS" width="48" src="https://raw.githubusercontent.com/awslabs/aws-icons-for-plantuml/main/dist/ApplicationIntegration/SimpleNotificationService.png" />
  &nbsp; OrderNotificationsTopic
</p>

<p align="center">
  <img alt="Amazon CloudWatch" width="48" src="https://raw.githubusercontent.com/awslabs/aws-icons-for-plantuml/main/dist/ManagementGovernance/CloudWatch.png" />
  &nbsp; CloudWatch dashboard and alarm monitor API activity, Lambda invocations, queue depth, and visible messages in the API ingress dead-letter queue.
</p>

Icon artwork: [AWS Icons for PlantUML](https://github.com/awslabs/aws-icons-for-plantuml).

## API order lifecycle

1. The client sends an order with a unique `Idempotency-Key`.
2. API Gateway validates the request and sends the JSON body and key to `EventDrivenAppOrderQueue`.
3. `ProcessOrderFunction` validates the order and claims the key in `EventDrivenAppIdempotencyTable` using a conditional write and time-limited lease.
4. A DynamoDB transaction writes the order to the existing Orders table and records the successful persistence.
5. The function publishes an `OrderProcessed` event to EventBridge and marks the idempotency record `COMPLETED`.
6. A repeated delivery with the same key and payload is acknowledged without creating the order again. Reusing the key for a different payload fails processing.
7. After three unsuccessful receives, SQS moves the message to `EventDrivenAppOrderDLQ`; a CloudWatch alarm fires when messages are visible there.

The API requires non-empty string `orderId` and `itemId` properties. Other order properties are allowed. The idempotency key must be 1–128 characters, start with a letter or digit, and contain only letters, numbers, `.`, `_`, `:`, or `-`. Reuse the same key when retrying the same request.

**Delivery is at least once, not exactly once.** Idempotency prevents duplicate business processing for the same key and request. Any external downstream system should also support deduplication or an outbox-style integration.

## AWS resources

| Service | Resources and purpose |
| --- | --- |
| API Gateway | `EventDrivenOrdersApi`, `POST /orders` endpoint and request validation |
| Amazon SQS | Encrypted API ingress queue and DLQ; EventBridge processing queue and its DLQ; EventBridge target delivery DLQ |
| AWS Lambda | `ProcessOrderFunction` handles API messages and legacy events; `CreateOrderFunction` writes and emits `OrderCreated` when invoked directly; `OrderNotificationFunction` logs order events |
| Amazon DynamoDB | Uses an existing Orders table; creates an on-demand, encrypted idempotency table with TTL |
| Amazon EventBridge | `OrdersEventBus`, `OrderCreatedRule`, and `OrderProcessedRule`; optional global endpoint |
| Amazon SNS / AWS KMS | Encrypted `OrderNotificationsTopic` and its rotating customer-managed KMS key; no topic subscriptions are created by this template |
| Amazon CloudWatch | `EventDrivenOrdersDashboard` and an alarm for visible messages in the API ingress DLQ |
| AWS IAM | Least-privilege service roles and a Lambda permissions boundary |

The API ingress queue retains messages for four days, has a 180-second visibility timeout, and redrives after three receives. The EventBridge processing queue has its own DLQ and a five-receive redrive policy. EventBridge target delivery failures use a separate delivery DLQ. The CloudWatch alarm in this template covers the API ingress DLQ; inspect the other DLQs separately.

The existing DynamoDB Orders table is not created by this template. It must use `orderId` and `itemId` as its key attributes. Set the `OrdersTableName` parameter if its name is not `Orders`.

The EventBridge global endpoint is optional and disabled by default. To enable it, configure `SecondaryEventBusRegion` and `SecondaryEventBusName`, and ensure the secondary event bus and required Route 53 health check are provisioned.

## Try the API

Replace `<api-id>` and `<region>` with the deployed API values:

```bash
curl -X POST "https://<api-id>.execute-api.<region>.amazonaws.com/Prod/orders" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: order-request-001" \
  -d '{"orderId":"order-001","itemId":"item-001","quantity":1}'
```

A successful response is HTTP `202` with a message that the order was accepted for processing. Get the URL and resource names from the CloudFormation stack outputs, including `OrdersApiUrl`, `EventDrivenAppOrderQueueUrl`, `EventDrivenAppOrderDLQUrl`, and `EventDrivenAppIdempotencyTableName`.

## Project layout

| Path | Purpose |
| --- | --- |
| `template.yaml` | AWS SAM / CloudFormation resources, API, queues, functions, IAM, alarms, dashboard, and outputs |
| `src/process-order/` | SQS order processor, idempotency logic, and Node.js tests |
| `src/save-order/` | Direct-invocation order persistence and EventBridge publisher |
| `src/order-notification/` | Event notification Lambda |
| `.github/workflows/deploy.yml` | GitHub Actions build and deployment workflow |

## Build and test

Prerequisites: Node.js 22, AWS CLI, AWS SAM CLI, and access to an AWS account with the required permissions. The deployment account must also have the existing Orders table and the `EventDrivenAppLambdaBoundary` IAM policy referenced by the template.

```bash
sam validate --lint --template-file template.yaml
sam build --template-file template.yaml
npm test --prefix src/process-order
```

## Deploy

For an interactive deployment:

```bash
sam deploy --guided
```

Provide the stack name, region, and deployment bucket. The stack creates IAM resources, including a named role, so approve the IAM capabilities requested by SAM (including `CAPABILITY_NAMED_IAM`).

Pushing to `main` triggers the GitHub Actions workflow. Before using it, configure the `AWS_ROLE_TO_ASSUME` repository secret and its AWS OIDC trust relationship. The workflow currently uses a fixed region, stack name, deployment bucket, and CloudFormation execution role in `.github/workflows/deploy.yml`; update those values for your AWS environment.

## Inspect and redrive failed messages

Check the `EventDrivenAppOrderDLQMessagesVisible` CloudWatch alarm and inspect `EventDrivenAppOrderDLQ` in the SQS console or with the AWS CLI. Correct the underlying issue before redriving messages to `EventDrivenAppOrderQueue`. Keep the original idempotency key so already-completed requests can be recognized safely.
