use aws_sdk_eventbridge::model::RuleState;
/// Self-deleting resources
///
/// These resources are not meant for abstracting the services they create. They are merely services that manage their own lifetimes, via the `Drop` trait.
use eyre::{Result, WrapErr};
use tokio::runtime::Handle;

use crate::constants::*;

// EventBridge rule

pub(crate) struct Rule<'h> {
    client: aws_sdk_eventbridge::Client,
    handle: &'h Handle,
    rule_name: String,
}

impl<'h> Rule<'h> {
    #[tracing::instrument(skip(rule_name, client, handle))]
    pub(crate) fn new(
        rule_name: impl Into<String>,
        source: Option<String>,
        client: aws_sdk_eventbridge::Client,
        handle: &'h Handle,
    ) -> Result<Self> {
        let rule_name = rule_name.into();
        tracing::debug!(?rule_name, "creating eventbridge rule");

        let pattern = serde_json::json!({
            "source": [source.unwrap()],
        });

        handle
            .block_on(async {
                client
                    .put_rule()
                    .name(&rule_name)
                    .event_pattern(pattern.to_string())
                    .state(RuleState::Enabled)
                    .send()
                    .await
            })
            .wrap_err("creating eventbridge rule")?;

        Ok(Self {
            rule_name,
            client,
            handle,
        })
    }

    #[tracing::instrument(skip(self))]
    pub(crate) fn arn(&self) -> Result<String> {
        let res = self
            .handle
            .block_on(async {
                tracing::debug!("fetching rule arn");
                self.client
                    .describe_rule()
                    .name(&self.rule_name)
                    .send()
                    .await
            })
            .wrap_err("fetching rule attributes")?;

        Ok(res.arn().unwrap().to_string())
    }
}

impl<'h> Drop for Rule<'h> {
    #[tracing::instrument(skip(self))]
    fn drop(&mut self) {
        tracing::debug!("dropping eventbridge rule");
        if let Err(e) = self
            .handle
            .block_on(async { self.client.delete_rule().name(&self.rule_name).send().await })
        {
            tracing::error!(?e, ?self.rule_name, "could not delete rule");
        }
    }
}

// rule targets
pub(crate) struct Target<'h> {
    client: aws_sdk_eventbridge::Client,
    handle: &'h Handle,
    rule_name: String,
    target_id: String,
}

impl<'h> Target<'h> {
    #[tracing::instrument(skip(target_id, rule_name, queue_arn, client, handle))]
    pub(crate) fn new(
        rule_name: impl Into<String>,
        queue_arn: impl Into<String>,
        target_id: impl Into<String>,
        client: aws_sdk_eventbridge::Client,
        handle: &'h Handle,
    ) -> Result<Self> {
        tracing::debug!("creating rule target");
        let rule_name = rule_name.into();
        let queue_arn = queue_arn.into();
        let target_id = target_id.into();

        let target = aws_sdk_eventbridge::model::Target::builder()
            .id(&target_id)
            .arn(queue_arn)
            .build();

        handle
            .block_on(async {
                client
                    .put_targets()
                    .rule(&rule_name)
                    .targets(target)
                    .send()
                    .await
            })
            .wrap_err("creating rule target")?;
        tracing::debug!("created rule target");
        Ok(Self {
            client,
            handle,
            rule_name,
            target_id,
        })
    }
}

impl<'h> Drop for Target<'h> {
    #[tracing::instrument(skip(self))]
    fn drop(&mut self) {
        tracing::debug!(%self.target_id, "deleting target");
        if let Err(e) = self.handle.block_on(async {
            self.client
                .remove_targets()
                .rule(&self.rule_name)
                .ids(&self.target_id)
                .send()
                .await
        }) {
            tracing::error!(?e, "could not remove target");
        }
        tracing::debug!(%self.target_id, "target deleted");
    }
}

// Queue

pub(crate) struct Queue<'h> {
    client: aws_sdk_sqs::Client,
    handle: &'h Handle,
    pub(crate) queue_url: String,
}

impl<'h> Queue<'h> {
    #[tracing::instrument(skip(name, client, handle))]
    pub(crate) fn new(
        name: impl AsRef<str>,
        client: aws_sdk_sqs::Client,
        handle: &'h Handle,
    ) -> Result<Self> {
        let name = name.as_ref();
        let queue_info = handle
            .block_on(async {
                client
                    .create_queue()
                    .queue_name(name)
                    .attributes("MessageRetentionPeriod".into(), VISIBILITY_TIMEOUT)
                    .attributes("ReceiveMessageWaitTimeSeconds".into(), WAIT_TIME)
                    .attributes("VisibilityTimeout".into(), VISIBILITY_TIMEOUT)
                    .send()
                    .await
            })
            .wrap_err("creating queue")?;
        let queue_url = queue_info.queue_url.expect("no queue url specified");
        tracing::debug!(?queue_url, "created queue");

        Ok(Self {
            client,
            handle,
            queue_url,
        })
    }

    pub(crate) fn arn(&self) -> Result<String> {
        let queue_attributes = self
            .handle
            .block_on(async {
                self.client
                    .get_queue_attributes()
                    .queue_url(&self.queue_url)
                    .attribute_names("QueueArn".into())
                    .send()
                    .await
            })
            .wrap_err("getting queue attributes")?;

        let attributes = queue_attributes.attributes.unwrap_or_default();
        let queue_arn = &attributes[&aws_sdk_sqs::model::QueueAttributeName::QueueArn];
        Ok(queue_arn.to_string())
    }
}

impl<'h> Drop for Queue<'h> {
    #[tracing::instrument(skip(self))]
    fn drop(&mut self) {
        tracing::debug!("dropping queue");
        let _ = self.handle.block_on(async {
            if let Err(e) = self
                .client
                .delete_queue()
                .queue_url(&self.queue_url)
                .send()
                .await
            {
                eyre::bail!("could not clean up temporary queue: {:?}", e);
            }

            Ok(())
        });
    }
}
