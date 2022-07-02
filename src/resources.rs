use eyre::{Result, WrapErr};
use tokio::runtime::Handle;

use crate::constants::*;

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
                    .attributes("MessageRetentionPeriod", VISIBILITY_TIMEOUT)
                    .attributes("ReceiveMessageWaitTimeSeconds", WAIT_TIME)
                    .attributes("VisibilityTimeout", VISIBILITY_TIMEOUT)
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
