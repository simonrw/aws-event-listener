use eyre::{Result, WrapErr};
use tokio::runtime::Handle;

use crate::constants::*;

// Policy
pub(crate) struct Policy<'h> {
    client: aws_sdk_iam::Client,
    handle: &'h Handle,
    role_name: String,
    policy_name: String,
}

impl<'h> Policy<'h> {
    pub(crate) fn new(
        policy_name: impl Into<String>,
        role_name: impl Into<String>,
        policy: serde_json::Value,
        client: aws_sdk_iam::Client,
        handle: &'h Handle,
    ) -> Result<Self> {
        let policy_name = policy_name.into();
        let role_name = role_name.into();

        // attach the policy
        let policy_string =
            serde_json::to_string(&policy).wrap_err("encoding policy JSON string")?;
        handle
            .block_on(async {
                client
                    .put_role_policy()
                    .role_name(role_name.clone())
                    .policy_name(policy_name.clone())
                    .policy_document(policy_string)
                    .send()
                    .await
            })
            .wrap_err("attaching policy")?;

        Ok(Self {
            client,
            handle,
            role_name,
            policy_name,
        })
    }
}

impl<'h> Drop for Policy<'h> {
    fn drop(&mut self) {
        self.handle.block_on(async {
            let _ = self.client.delete_role_policy()
                .role_name(&self.role_name)
                .policy_name(&self.policy_name)
                .send().await;
        });
    }
}

// Queue

pub(crate) struct Queue<'h> {
    client: aws_sdk_sqs::Client,
    handle: &'h Handle,
    pub(crate) queue_url: Option<String>,
}

impl<'h> Queue<'h> {
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
        let queue_url = Some(queue_info.queue_url).expect("no queue url specified");

        Ok(Self {
            client,
            handle,
            queue_url,
        })
    }
}

impl<'h> Drop for Queue<'h> {
    fn drop(&mut self) {
        if let Some(queue_url) = self.queue_url.take() {
            let _ = self.handle.block_on(async {
                if let Err(e) = self.client.delete_queue().queue_url(queue_url).send().await {
                    eyre::bail!("could not clean up temporary queue: {:?}", e);
                }

                Ok(())
            });
        }
    }
}
