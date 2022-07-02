use eyre::{Result, WrapErr};
use serde::Deserialize;
use std::collections::HashMap;
use std::time::Duration;
use structopt::StructOpt;
use tokio::sync::mpsc;

mod constants;
mod resources;

use constants::*;
use resources::Queue;

#[derive(StructOpt)]
enum Opts {
    Sns {
        #[structopt(short, long)]
        topic: String,

        #[structopt(short, long)]
        prefix: Option<Vec<String>>,
    },
}

async fn gen_messages(
    sqs_client: &aws_sdk_sqs::Client,
    queue_url: &str,
    tx: mpsc::Sender<aws_sdk_sqs::model::Message>,
) -> Result<()> {
    // poll for messages
    loop {
        tracing::trace!("poll loop");
        match sqs_client
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
        {
            Ok(messages_response) => {
                tracing::trace!("got messages");
                let messages = messages_response.messages.unwrap_or_default();
                tracing::debug!(num_messages = %messages.len(), "found messages");
                for message in messages {
                    let _ = tx.send(message).await;
                }
            }
            Err(e) => {
                tracing::warn!("got receive message error: {:?}", e);
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        }
    }
}

#[derive(Deserialize, Debug)]
struct Attribute {
    #[serde(rename = "Value")]
    value: String,
}

#[derive(Deserialize, Debug)]
struct Payload {
    #[serde(rename = "Message")]
    message: String,
    #[serde(rename = "MessageAttributes")]
    attributes: Option<HashMap<String, Attribute>>,
}

fn print_message(message: aws_sdk_sqs::model::Message) -> Result<()> {
    if let Some(body) = message.body {
        let payload: Payload = serde_json::from_str(&body).wrap_err("decoding JSON message")?;
        let attributes = payload.attributes.unwrap_or_default();
        tracing::trace!(?attributes, "found message attributes");

        let data: serde_json::Value =
            serde_json::from_str(&payload.message).wrap_err("decoding JSON data")?;

        if let Some(event_type) = attributes
            .get("type")
            .map(|attr| attr.value.clone())
            .or_else(|| attributes.get("event_name").map(|attr| attr.value.clone()))
        {
            println!("== {} ==", event_type);
        }

        // unwrap is safe because the object was JSON to start with
        println!("{}\n", colored_json::to_colored_json_auto(&data).unwrap());
    }
    Ok(())
}

fn main() -> Result<()> {
    color_eyre::install()?;
    tracing_subscriber::fmt::init();

    let opts = Opts::from_args();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let config = runtime.block_on(aws_config::load_from_env());
    let sqs_client = aws_sdk_sqs::Client::new(&config);
    let sns_client = aws_sdk_sns::Client::new(&config);

    match opts {
        Opts::Sns { topic, prefix } => {
            let queue_name = format!("snslistener-{}", uuid::Uuid::new_v4());
            tracing::info!(%queue_name, "creating queue");

            let queue = Queue::new(&queue_name, sqs_client.clone(), runtime.handle())?;
            let queue_url = queue.queue_url.clone().unwrap();
            tracing::info!("worker closure");

            let queue_attributes = runtime
                .block_on(async {
                    sqs_client
                        .get_queue_attributes()
                        .queue_url(&queue_url)
                        .attribute_names("QueueArn")
                        .send()
                        .await
                })
                .wrap_err("getting queue attributes")?;

            let attributes = queue_attributes.attributes.unwrap_or_default();
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
                    "Condition": {"ArnEquals": {"aws:SourceArn": topic}},
                }],
            });
            let policy_text = policy.to_string();
            tracing::debug!(policy = %policy_text, "generated policy");

            runtime
                .block_on(async {
                    sqs_client
                        .set_queue_attributes()
                        .queue_url(&queue_url)
                        .attributes("Policy", policy_text)
                        .send()
                        .await
                })
                .wrap_err("setting queue attributes")?;

            tracing::info!("subscribing queue to topic");
            let subscribe_res = runtime
                .block_on(async {
                    sns_client
                        .subscribe()
                        .topic_arn(&topic)
                        .protocol("sqs")
                        .endpoint(queue_arn)
                        .return_subscription_arn(true)
                        .send()
                        .await
                })
                .wrap_err("subscribing to topic")?;
            if let Some(prefixes) = prefix {
                match subscribe_res.subscription_arn {
                    Some(subscription_arn) => {
                        tracing::debug!(?prefixes, %subscription_arn, "setting filter parameters");

                        let policy_prefixes: Vec<_> = prefixes
                            .into_iter()
                            .map(|p| serde_json::json!({ "prefix": p }))
                            .collect();
                        let filter_policy = serde_json::json!({ "event_name": policy_prefixes });
                        runtime
                            .block_on(async {
                                sns_client
                                    .set_subscription_attributes()
                                    .subscription_arn(subscription_arn)
                                    .attribute_name("FilterPolicy")
                                    .attribute_value(filter_policy.to_string())
                                    .send()
                                    .await
                            })
                            .wrap_err("setting subscription attributes")?;
                    }
                    None => eyre::bail!("did not receive subscription arn"),
                }
            }

            let (tx, mut messages_rx) = mpsc::channel(16);
            let message_subscriber = runtime.spawn(async move {
                tracing::trace!("spawned background task");
                gen_messages(&sqs_client, &queue_url, tx).await.unwrap();
            });

            let (tx, mut ctrlc_rx) = mpsc::channel(1);
            ctrlc::set_handler(move || {
                let _ = tx.blocking_send(());
            })
            .wrap_err("setting ctrl-c handler")?;

            println!("Listening for messages...");
            runtime.block_on(async {
                loop {
                    tokio::select! {
                        message = messages_rx.recv() => {
                            if let Some(message) = message {
                                if let Err(e) = print_message(message) {
                                    tracing::warn!(error = ?e, "error printing message");
                                }
                            }
                        }
                        _ = ctrlc_rx.recv() => {
                            message_subscriber.abort();
                            break
                        }
                    }
                }
            });

            Ok(())
        }
    }
}
