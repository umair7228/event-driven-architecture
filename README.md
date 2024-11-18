# Event-Driven Architecture Project with AWS Services

This project demonstrates an event-driven architecture using AWS services to build a real-time data processing pipeline. The architecture is designed to process events generated in an S3 bucket and uses services like SNS, SQS, and Lambda for reliable, scalable event processing.

## Architecture Overview

![architecture-diagram](https://github.com/user-attachments/assets/e9727a15-e073-4a4f-a1fe-93d641807387)

### Components and Roles

![components](https://github.com/user-attachments/assets/449da32b-cf2d-4772-b74c-719ea8671421)

- **Event Producer**: S3 acts as the event producer, generating events whenever an object is created, modified, or deleted.
- **Event Ingestion**: SNS acts as the event ingestion layer, capturing events from S3 and distributing them to subscribers.
- **Event Stream**: SQS functions as the event stream, buffering and storing events from SNS to ensure reliable, sequential processing.
- **Event Consumer**: Lambda serves as the event consumer, triggered by events in SQS to process data and execute business logic.
- **Event Sink**: The final S3 bucket acts as the event sink, storing processed data from Lambda and completing the pipeline loop.

### Workflow

1. **Source S3 Bucket**: An event is triggered when a new object is added, modified, or deleted in the bucket.
2. **SNS Notification**: Receives the event from S3 and distributes it to the SQS queue.
3. **SQS Queue**: Buffers the event and ensures it is delivered to the consumer in a reliable and ordered manner.
4. **Lambda Processing**: The Lambda function is triggered by SQS to process the event, perform transformations, and move the result to the target S3 bucket.
5. **Target S3 Bucket**: Stores the processed output, completing the data processing workflow.
