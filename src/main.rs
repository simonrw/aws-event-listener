use eyre::{Result, WrapErr};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use structopt::StructOpt;
use tokio::sync::mpsc;

mod constants;
mod resources;

use constants::*;
use resources::Queue;
use resources::Rule;

#[derive(StructOpt)]
struct Opts {
    #[structopt(long)]
    endpoint_url: Option<String>,

    #[structopt(subcommand)]
    mode: Mode,
}

#[derive(StructOpt)]
enum Mode {
    EventBridge {
        #[structopt(short, long)]
        source: Option<String>,

        #[structopt(short, long)]
        pattern: Option<serde_json::Value>,

        #[structopt(short, long)]
        bus: Option<String>,
    },
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
            .attribute_names("SentTimestamp".into())
            .attribute_names("ApproximateFirstReceiveTimestamp".into())
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
#[serde(untagged)]
enum Payload {
    SnsMessage {
        #[serde(rename = "Message")]
        message: String,
        #[serde(rename = "MessageAttributes")]
        attributes: Option<HashMap<String, Attribute>>,
    },
    EventBridgeMessage(EventBridgeMessage),
}

#[derive(Serialize, Deserialize, Debug)]
struct EventBridgeMessage {
    source: String,
    detail: serde_json::Value,
    time: String,
    resources: Vec<String>,
    #[serde(rename = "detail-type")]
    detail_type: String,
}

fn print_message(message: aws_sdk_sqs::model::Message) -> Result<bool> {
    if let Some(body) = message.body {
        let payload: Payload = serde_json::from_str(&body).wrap_err("decoding JSON message")?;
        match payload {
            Payload::SnsMessage {
                message,
                attributes,
            } => {
                let attributes = attributes.unwrap_or_default();
                tracing::trace!(?attributes, "found message attributes");

                let data: serde_json::Value =
                    serde_json::from_str(&message).wrap_err("decoding JSON data")?;

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
            Payload::EventBridgeMessage(ref message) => {
                let value = serde_json::to_value(message).unwrap();
                println!("{}\n", colored_json::to_colored_json_auto(&value).unwrap());
            }
        }
    }
    Ok(false)
}

fn handle_sns<F>(
    context: Context,
    topic: String,
    prefix: Option<Vec<String>>,
    on_message: F,
) -> Result<()>
where
    F: FnOnce(aws_sdk_sqs::model::Message) -> Result<bool> + Clone,
{
    let Context {
        runtime,
        sqs_client,
        sns_client,
        ..
    } = &context;

    let queue_name = format!("snslistener-{}", uuid::Uuid::new_v4());
    tracing::info!(%queue_name, "creating queue");

    let queue = Queue::new(&queue_name, sqs_client.clone(), runtime.handle())?;
    let queue_url = queue.queue_url.clone();
    tracing::info!("worker closure");

    let queue_arn = queue.arn().wrap_err("fetching queue arn")?;
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
                .attributes("Policy".into(), policy_text)
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

    subscribe_to_messages(context, queue_url, on_message)
}

#[derive(Clone)]
struct Context<'r> {
    runtime: &'r tokio::runtime::Runtime,
    sqs_client: aws_sdk_sqs::Client,
    sns_client: aws_sdk_sns::Client,
    eventbridge_client: aws_sdk_eventbridge::Client,
}

#[tracing::instrument(skip(context, on_message))]
fn handle_eventbridge<F>(
    context: Context,
    source: Option<String>,
    pattern: Option<serde_json::Value>,
    bus: Option<String>,
    on_message: F,
) -> Result<()>
where
    F: FnOnce(aws_sdk_sqs::model::Message) -> Result<bool> + Clone,
{
    let Context {
        runtime,
        sqs_client,
        eventbridge_client,
        ..
    } = &context;

    let id = uuid::Uuid::new_v4();
    let queue_name = format!("snslistener-{}", id);
    tracing::info!(%queue_name, "creating queue");

    let queue = Queue::new(&queue_name, sqs_client.clone(), runtime.handle())?;
    let queue_url = queue.queue_url.clone();
    let queue_arn = queue.arn().wrap_err("fetching queue arn")?;

    // create the eventbridge rule
    let rule_name = format!("sqslistener-rule-{}", id);
    if source.is_none() && pattern.is_none() {
        eyre::bail!("must specify either `source` or `pattern`");
    }

    let pattern = if let Some(source) = source {
        serde_json::json!({
            "source": [source],
        })
    } else {
        pattern.unwrap()
    };
    tracing::debug!(%pattern, "adding rule pattern");

    let rule = Rule::new(
        &rule_name,
        pattern,
        eventbridge_client.clone(),
        runtime.handle(),
    )?;

    let rule_arn = rule.arn()?;
    let target_id = format!("sqslistener-target-{}", id);
    let _target = crate::resources::Target::new(
        &rule_name,
        &queue_arn,
        target_id,
        eventbridge_client.clone(),
        runtime.handle(),
    )?;

    // allow eventbridge to send messages to the queue

    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Sid": "SNSSubscriberWriteToQueue",
            "Effect": "Allow",
            "Principal": {"Service": "events.amazonaws.com"},
            "Action": "SQS:SendMessage",
            "Resource": queue_arn,
            "Condition": {"ArnEquals": {"aws:SourceArn": rule_arn}},
        }],
    });
    tracing::debug!(?policy, "attaching policy");

    runtime
        .block_on(async {
            sqs_client
                .set_queue_attributes()
                .queue_url(&queue_url)
                .attributes("Policy".into(), policy.to_string())
                .send()
                .await
        })
        .wrap_err("setting queue attributes")?;

    subscribe_to_messages(context, queue_url, on_message)
}

fn subscribe_to_messages<F>(context: Context<'_>, queue_url: String, callback: F) -> Result<()>
where
    F: FnOnce(aws_sdk_sqs::model::Message) -> Result<bool> + Clone,
{
    let Context {
        runtime,
        sqs_client,
        ..
    } = context;
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
                        match (callback.clone())(message) {
                            Ok(true) => break,
                            Ok(false) => {},
                            Err(e) => tracing::warn!(error=?e, "error in message handler"),
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

async fn load_sqs_config(endpoint_url: &Option<String>) -> aws_types::SdkConfig {
    if let Some(url) = endpoint_url {
        aws_config::from_env()
            .endpoint_resolver(aws_sdk_sqs::Endpoint::immutable(
                url.parse().expect("invalid URI"),
            ))
            .load()
            .await
    } else {
        aws_config::load_from_env().await
    }
}

async fn load_sns_config(endpoint_url: &Option<String>) -> aws_types::SdkConfig {
    if let Some(url) = endpoint_url {
        aws_config::from_env()
            .endpoint_resolver(aws_sdk_sns::Endpoint::immutable(
                url.parse().expect("invalid URI"),
            ))
            .load()
            .await
    } else {
        aws_config::load_from_env().await
    }
}

async fn load_eventbridge_config(endpoint_url: &Option<String>) -> aws_types::SdkConfig {
    if let Some(url) = endpoint_url {
        aws_config::from_env()
            .endpoint_resolver(aws_sdk_eventbridge::Endpoint::immutable(
                url.parse().expect("invalid URI"),
            ))
            .load()
            .await
    } else {
        aws_config::load_from_env().await
    }
}

fn main() -> Result<()> {
    color_eyre::install()?;
    tracing_subscriber::fmt::init();

    let opts = Opts::from_args();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let sqs_client = {
        let config = runtime.block_on(load_sqs_config(&opts.endpoint_url));
        aws_sdk_sqs::Client::new(&config)
    };
    let sns_client = {
        let config = runtime.block_on(load_sns_config(&opts.endpoint_url));
        aws_sdk_sns::Client::new(&config)
    };
    let eventbridge_client = {
        let config = runtime.block_on(load_eventbridge_config(&opts.endpoint_url));
        aws_sdk_eventbridge::Client::new(&config)
    };

    let context = Context {
        runtime: &runtime,
        sqs_client,
        sns_client,
        eventbridge_client,
    };

    match opts.mode {
        Mode::EventBridge {
            source,
            pattern,
            bus,
        } => handle_eventbridge(context.clone(), source, pattern, bus, print_message),
        Mode::Sns { topic, prefix } => handle_sns(context.clone(), topic, prefix, print_message),
    }
}

#[cfg(test)]
mod tests {
    use tokio::runtime::Runtime;

    use super::*;
    #[cfg(feature = "integration_test_support")]
    use crate::resources::integration_test_support::Topic;

    #[test]
    #[cfg(feature = "integration")]
    fn integration() {
        use std::{
            sync::{atomic::Ordering, Arc},
            thread,
        };

        // assume a localstack instance is running
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        let context = local_context(&runtime);
        let topic = crate::resources::integration_test_support::Topic::new(
            "topic",
            context.sns_client.clone(),
            runtime.handle(),
        )
        .unwrap();

        let got_message = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let cb_flag = Arc::clone(&got_message);
        let callback = move |_| {
            println!("got message");
            cb_flag.store(true, Ordering::SeqCst);
            println!("shutting down listener");
            Ok(true)
        };

        thread::scope(|s| {
            s.spawn(|| {
                handle_sns(context.clone(), topic.arn.clone(), None, callback).unwrap();
            });

            thread::sleep(Duration::from_secs(1));
            println!("sending sns message");

            // wait for the resources to be created
            runtime.block_on(async {
                context
                    .sns_client
                    .publish()
                    .topic_arn(topic.arn.clone())
                    .message(r#"{"message": "hello world"}"#)
                    .send()
                    .await
                    .unwrap();
            });
        });

        assert!(got_message.load(Ordering::SeqCst));
    }

    #[test]
    fn simple() {
        assert_eq!(1, 1);
    }

    #[allow(dead_code)]
    fn local_context(runtime: &Runtime) -> Context<'_> {
        let endpoint_url = Some("http://localhost:4566".to_string());
        let sqs_client = {
            let config = runtime.block_on(load_sqs_config(&endpoint_url));
            aws_sdk_sqs::Client::new(&config)
        };
        let sns_client = {
            let config = runtime.block_on(load_sns_config(&endpoint_url));
            aws_sdk_sns::Client::new(&config)
        };
        let eventbridge_client = {
            let config = runtime.block_on(load_eventbridge_config(&endpoint_url));
            aws_sdk_eventbridge::Client::new(&config)
        };

        Context {
            runtime,
            sqs_client,
            sns_client,
            eventbridge_client,
        }
    }
}
