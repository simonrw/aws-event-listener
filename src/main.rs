use eyre::{Result, WrapErr};
use serde::Deserialize;
use std::collections::HashMap;
use std::future::Future;
use structopt::StructOpt;
use tokio::sync::mpsc;

const MAX_NUMBER_OF_MESSAGES: &str = "10";
const VISIBILITY_TIMEOUT: &str = "60";
const WAIT_TIME: &str = "5";

#[derive(StructOpt)]
struct Opts {
    #[structopt(short, long)]
    topic: String,

    #[structopt(short, long)]
    prefix: Option<String>,
}

async fn with_cleanup_queue<T, F>(
    sqs_client: aws_sdk_sqs::Client,
    queue_url: impl Into<String>,
    f: F,
) -> Result<()>
where
    F: FnOnce(aws_sdk_sqs::Client, String) -> T,
    T: Future<Output = Result<()>>,
{
    let queue_url = queue_url.into();
    let res = f(sqs_client.clone(), queue_url.clone()).await;

    tracing::debug!(%queue_url, "cleaning up queue");
    if let Err(e) = sqs_client.delete_queue().queue_url(&queue_url).send().await {
        eyre::bail!("could not clean up temporary queue: {:?}", e);
    }

    res
}

async fn gen_messages(
    sqs_client: &aws_sdk_sqs::Client,
    queue_url: &str,
    tx: mpsc::Sender<aws_sdk_sqs::model::Message>,
) -> Result<()> {
    // poll for messages
    loop {
        tracing::trace!("poll loop");
        let messages_response = sqs_client
            .receive_message()
            .queue_url(queue_url)
            .attribute_names("SentTimestamp")
            .attribute_names("ApproximateFirstReceiveTimestamp")
            .message_attribute_names("ALL")
            .max_number_of_messages(MAX_NUMBER_OF_MESSAGES.parse().unwrap())
            .visibility_timeout(VISIBILITY_TIMEOUT.parse().unwrap())
            .wait_time_seconds(WAIT_TIME.parse().unwrap())
            .send()
            .await
            .wrap_err("receiving messages")?;
        tracing::trace!("got messages");

        let messages = messages_response.messages.unwrap_or_else(Vec::new);
        tracing::debug!(num_messages = %messages.len(), "found messages");
        for message in messages {
            let _ = tx.send(message).await;
        }
    }
}

#[derive(Deserialize)]
struct Payload {
    #[serde(rename = "Message")]
    message: String,
}

fn print_message(message: aws_sdk_sqs::model::Message) -> Result<()> {
    if let Some(body) = message.body {
        let payload: Payload = serde_json::from_str(&body).wrap_err("decoding JSON message")?;
        let data: serde_json::Value =
            serde_json::from_str(&payload.message).wrap_err("decoding JSON data")?;

        // unwrap is safe because the object was JSON to start with
        println!("{}", serde_json::to_string_pretty(&data).unwrap());
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    tracing_subscriber::fmt::init();

    let opts = Opts::from_args();

    let config = aws_config::load_from_env().await;
    let sqs_client = aws_sdk_sqs::Client::new(&config);
    let sns_client = aws_sdk_sns::Client::new(&config);

    let queue_name = format!("snslistener-{}", uuid::Uuid::new_v4());
    tracing::info!(%queue_name, "creating queue");

    let queue_info = sqs_client
        .create_queue()
        .queue_name(&queue_name)
        .attributes("MessageRetentionPeriod", VISIBILITY_TIMEOUT)
        .attributes("ReceiveMessageWaitTimeSeconds", WAIT_TIME)
        .attributes("VisibilityTimeout", VISIBILITY_TIMEOUT)
        .send()
        .await
        .wrap_err("creating queue")?;

    let res = match queue_info.queue_url {
        Some(queue_url) => with_cleanup_queue(
            sqs_client,
            queue_url,
            move |sqs_client, queue_url| async move {
                tracing::info!("worker closure");

                let queue_attributes = sqs_client
                    .get_queue_attributes()
                    .queue_url(&queue_url)
                    .attribute_names("QueueArn")
                    .send()
                    .await
                    .wrap_err("getting queue attributes")?;

                let attributes = queue_attributes.attributes.unwrap_or_else(HashMap::new);
                let queue_arn = &attributes[&aws_sdk_sqs::model::QueueAttributeName::QueueArn];
                tracing::debug!(%queue_arn, "found queue arn");

                let policy = serde_json::json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Sid": "SNSSubscriberWriteToQueue",
                        "Effect": "Allow",
                        "Principal": {"AWS": "*"},
                        "Action": "SQS:SendMessage",
                        "Resource": queue_arn,
                        "Condition": {"ArnEquals": {"aws:SourceArn": opts.topic.clone()}},
                    }],
                });
                let policy_text = policy.to_string();
                tracing::debug!(policy = %policy_text, "generated policy");

                sqs_client
                    .set_queue_attributes()
                    .queue_url(&queue_url)
                    .attributes("Policy", policy_text)
                    .send()
                    .await
                    .wrap_err("setting queue attributes")?;

                tracing::info!("subscribing queue to topic");
                let subscribe_res = sns_client
                    .subscribe()
                    .topic_arn(&opts.topic)
                    .protocol("sqs")
                    .endpoint(queue_arn)
                    .return_subscription_arn(true)
                    .send()
                    .await
                    .wrap_err("subscribing to topic")?;
                if let Some(prefix) = opts.prefix {
                    match subscribe_res.subscription_arn {
                        Some(subscription_arn) => {
                            tracing::debug!("setting filter parameters");

                            let filter_policy = serde_json::json!({
                                "event_name": [
                                    {"prefix": prefix}
                                ]
                            });
                            sns_client
                                .set_subscription_attributes()
                                .subscription_arn(subscription_arn)
                                .attribute_name("FilterPolicy")
                                .attribute_value(filter_policy.to_string())
                                .send()
                                .await
                                .wrap_err("setting subscription attributes")?;
                        }
                        None => eyre::bail!("did not receive subscription arn"),
                    }
                }

                let (tx, mut messages_rx) = mpsc::channel(16);
                let message_subscriber = tokio::spawn(async move {
                    tracing::trace!("spawned background task");
                    gen_messages(&sqs_client, &queue_url, tx).await.unwrap();
                });

                let (tx, mut ctrlc_rx) = mpsc::channel(1);
                ctrlc::set_handler(move || {
                    let _ = tx.blocking_send(());
                })
                .wrap_err("setting ctrl-c handler")?;

                loop {
                    tokio::select! {
                        message = messages_rx.recv() => {
                            if let Some(message) = message {
                                print_message(message).wrap_err("printing message")?;
                            }
                        }
                        _ = ctrlc_rx.recv() => {
                            message_subscriber.abort();
                            break
                        }
                    }
                }

                Ok(())
            },
        )
        .await
        .wrap_err("cleaning up queue"),
        None => Err(eyre::eyre!(
            "could not determine queue url; cannot continue"
        )),
    };

    if let Err(e) = res {
        eyre::bail!("error subscribing: {:?}", e);
    }

    Ok(())
}
