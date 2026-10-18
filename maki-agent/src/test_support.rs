//! Scripted providers shared by in-crate unit tests and, through the
//! `test-support` feature, by integration tests.

use std::sync::{Arc, Mutex};

use maki_providers::provider::{BoxFuture, Provider};
use maki_providers::{AgentError, Message, Model, ProviderEvent, RequestOptions, StreamResponse};
use maki_storage::id::SessionRef;
use serde_json::Value;

/// Messages are kept serialized, because the cache and the thinking
/// bindings compare bytes, not values.
pub struct CapturedRequest {
    pub model: String,
    pub system: String,
    pub tools: Value,
    pub messages: Vec<Value>,
}

pub struct MockProvider {
    pub responses: Mutex<Vec<StreamResponse>>,
    pub requests: Arc<Mutex<Vec<CapturedRequest>>>,
    pub on_request: Option<Box<dyn Fn() + Send + Sync>>,
}

impl MockProvider {
    pub fn new(responses: Vec<StreamResponse>) -> Self {
        Self {
            responses: Mutex::new(responses),
            requests: Arc::default(),
            on_request: None,
        }
    }
}

impl Provider for MockProvider {
    fn stream_message<'a>(
        &'a self,
        model: &'a Model,
        messages: &'a [Message],
        system: &'a str,
        tools: &'a Value,
        _: &'a flume::Sender<ProviderEvent>,
        _: RequestOptions,
        _: Option<&'a SessionRef>,
    ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
        Box::pin(async {
            self.requests.lock().unwrap().push(CapturedRequest {
                model: model.id.clone(),
                system: system.to_owned(),
                tools: tools.clone(),
                messages: messages
                    .iter()
                    .map(|m| serde_json::to_value(m).unwrap())
                    .collect(),
            });
            if let Some(hook) = &self.on_request {
                hook();
            }
            let mut responses = self.responses.lock().unwrap();
            assert!(!responses.is_empty(), "MockProvider: no more responses");
            Ok(responses.remove(0))
        })
    }

    fn list_models(&self) -> BoxFuture<'_, Result<Vec<maki_providers::ModelInfo>, AgentError>> {
        Box::pin(async { unimplemented!() })
    }
}

/// Streams `delta` (if any), fires `cancel_after_delta` (if any),
/// then fails with `fail_status` or hangs until cancelled.
#[derive(Default)]
pub struct StubStreamProvider {
    pub delta: Option<&'static str>,
    pub cancel_after_delta: Mutex<Option<crate::cancel::CancelTrigger>>,
    pub fail_status: Option<u16>,
}

impl Provider for StubStreamProvider {
    fn stream_message<'a>(
        &'a self,
        _: &'a Model,
        _: &'a [Message],
        _: &'a str,
        _: &'a Value,
        ptx: &'a flume::Sender<ProviderEvent>,
        _: RequestOptions,
        _: Option<&'a SessionRef>,
    ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
        Box::pin(async move {
            if let Some(text) = self.delta {
                ptx.send(ProviderEvent::TextDelta { text: text.into() })
                    .unwrap();
            }
            if let Some(trigger) = self.cancel_after_delta.lock().unwrap().take() {
                trigger.cancel();
            }
            match self.fail_status {
                Some(status) => Err(AgentError::api(status, "stub")),
                None => futures_lite::future::pending().await,
            }
        })
    }

    fn list_models(&self) -> BoxFuture<'_, Result<Vec<maki_providers::ModelInfo>, AgentError>> {
        Box::pin(async { unimplemented!() })
    }
}
