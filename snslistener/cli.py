import argparse
import json
import logging
import sys
import uuid

import boto3
from pygments import formatters, highlight, lexers

logging.basicConfig(level=logging.WARNING)

MAX_NUMBER_OF_MESSAGES = 10
VISIBILITY_TIMEOUT = 60
WAIT_TIME = 20


def pretty_print(event_type, d):
    text = json.dumps(d, indent=2, sort_keys=True)
    if event_type is not None:
        print(f"== {event_type} ==")
    print(highlight(text, lexers.JsonLexer(), formatters.TerminalFormatter()))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--topic", nargs="*", default=[])
    parser.add_argument("-p", "--prefix", nargs="+", default=[])
    parser.add_argument("-v", "--verbose", action="count", default=0)
    args = parser.parse_args()

    if len(args.topic) > 1 and len(args.prefix) > 1:
        print("Multiple topics and prefixes not supported", file=sys.stderr)
        sys.exit(1)

    logger = logging.getLogger("snslistener")
    if args.verbose == 1:
        logger.setLevel(logging.INFO)
    elif args.verbose > 1:
        logger.setLevel(logging.DEBUG)

    sns_client = boto3.client("sns")
    sqs_client = boto3.client("sqs")

    queue_name = f"snslistener-{uuid.uuid4()}"
    logger.debug(f"creating queue {queue_name}")
    queue_info = sqs_client.create_queue(
        QueueName=queue_name,
        Attributes={
            "MessageRetentionPeriod": str(VISIBILITY_TIMEOUT),
            "ReceiveMessageWaitTimeSeconds": str(WAIT_TIME),
            "VisibilityTimeout": str(VISIBILITY_TIMEOUT),
        },
    )

    subscription_arn = None
    queue_url = None
    try:
        queue_url = queue_info["QueueUrl"]
        queue_attributes = sqs_client.get_queue_attributes(
            QueueUrl=queue_url, AttributeNames=["QueueArn"]
        )
        queue_arn = queue_attributes["Attributes"]["QueueArn"]
        logger.debug(f"queue arn: {queue_arn}")

        policy = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "PIXMsgListenerWriteToQueue",
                        "Effect": "Allow",
                        "Principal": {"AWS": "*"},
                        "Action": "SQS:SendMessage",
                        "Resource": queue_arn,
                        "Condition": {"ArnEquals": {"aws:SourceArn": topic_arn}},
                    }
                    for topic_arn in args.topic
                ],
            }
        )

        # give SNS permissions to write to the queue
        sqs_client.set_queue_attributes(
            QueueUrl=queue_url,
            Attributes={"Policy": policy},
        )

        logger.info("creating subscriptions")
        for topic in args.topic:
            logger.debug(f"subscribing to {topic}")
            res = sns_client.subscribe(
                TopicArn=topic,
                Protocol="sqs",
                Endpoint=queue_arn,
                ReturnSubscriptionArn=True,
            )
            subscription_arn = res["SubscriptionArn"]

            if args.prefix and subscription_arn is not None:
                logger.debug(f"setting filter prefix to include {args.prefix}")
                sns_client.set_subscription_attributes(
                    SubscriptionArn=subscription_arn,
                    AttributeName="FilterPolicy",
                    AttributeValue=json.dumps(
                        {"event_name": [{"prefix": prefix} for prefix in args.prefix]}
                    ),
                )

        print("Listening for messages...")
        while True:
            logger.debug("poll loop")
            get_messages_response = sqs_client.receive_message(
                QueueUrl=queue_url,
                AttributeNames=["SentTimestamp", "ApproximateFirstReceiveTimestamp"],
                MessageAttributeNames=["All"],
                MaxNumberOfMessages=MAX_NUMBER_OF_MESSAGES,
                VisibilityTimeout=VISIBILITY_TIMEOUT,
                WaitTimeSeconds=WAIT_TIME,
            )

            messages = get_messages_response.get("Messages", [])
            for message in messages:
                payload = json.loads(message["Body"])

                attributes = payload["MessageAttributes"]
                event_type = attributes.get("event_name", {}).get(
                    "Value", None
                ) or attributes.get("type", {}).get("Value", None)

                content = json.loads(payload["Message"])
                pretty_print(event_type, content)

    finally:
        if subscription_arn is not None:
            logger.info("unsubscribing from topic")
            sns_client.unsubscribe(SubscriptionArn=subscription_arn)

        if queue_url is not None:
            logger.info("deleting queue")
            sqs_client.delete_queue(QueueUrl=queue_url)
