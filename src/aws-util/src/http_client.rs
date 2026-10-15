// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An AWS SDK HTTP client that speaks only HTTP/1.1.
//!
//! The SDK's own hyper 1 client offers HTTP/2 in the TLS handshake and has no
//! setting to stop it. A server that accepts HTTP/2, like GCS, then gets every
//! request to a host over one connection. Throughput is capped at what that
//! connection carries, and requests queue behind the server's limit on
//! concurrent streams. Over HTTP/1.1, each in-flight request has its own
//! connection.
//!
//! Otherwise this follows the SDK's client: rustls on aws-lc-rs with the SDK's
//! cipher suites, the system's root certificates, `TCP_NODELAY`, connect and
//! read timeouts, connection poisoning, and the SDK's sorting of errors into
//! retryable and not.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::error::Error;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, LazyLock, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use aws_smithy_async::rt::sleep::{AsyncSleep, SharedAsyncSleep, default_async_sleep};
use aws_smithy_runtime_api::box_error::BoxError;
use aws_smithy_runtime_api::client::connection::{CaptureSmithyConnection, ConnectionMetadata};
use aws_smithy_runtime_api::client::connector_metadata::ConnectorMetadata;
use aws_smithy_runtime_api::client::http::{
    HttpClient, HttpConnector, HttpConnectorFuture, HttpConnectorSettings, SharedHttpConnector,
};
use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
use aws_smithy_runtime_api::client::result::ConnectorError;
use aws_smithy_runtime_api::client::runtime_components::{
    RuntimeComponents, RuntimeComponentsBuilder,
};
use aws_smithy_types::body::SdkBody;
use aws_smithy_types::config_bag::ConfigBag;
use aws_smithy_types::error::display::DisplayErrorContext;
use aws_smithy_types::retry::ErrorKind;
use http::{Extensions, Uri};
use hyper_rustls::{ConfigBuilderExt, HttpsConnector, MaybeHttpsStream};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::dns::Name;
use hyper_util::client::legacy::connect::{
    CaptureConnection, HttpConnector as TcpConnector, HttpInfo, capture_connection,
};
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use tokio::net::TcpStream;
use tower::Service;
use tracing::warn;

/// The cipher suites the SDK's client allows.
const CIPHER_SUITES: &[rustls::CipherSuite] = &[
    rustls::CipherSuite::TLS13_AES_256_GCM_SHA384,
    rustls::CipherSuite::TLS13_AES_128_GCM_SHA256,
    rustls::CipherSuite::TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
    rustls::CipherSuite::TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
    rustls::CipherSuite::TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
    rustls::CipherSuite::TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
    rustls::CipherSuite::TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
];

/// Built once per process, since it loads the system's root certificates.
static TLS_CONFIG: LazyLock<rustls::ClientConfig> = LazyLock::new(|| {
    let mut provider = rustls::crypto::aws_lc_rs::default_provider();
    provider
        .cipher_suites
        .retain(|suite| CIPHER_SUITES.contains(&suite.suite()));
    let builder = rustls::ClientConfig::builder_with_provider(Arc::new(provider))
        .with_safe_default_protocol_versions()
        .expect("aws-lc-rs supports the default protocol versions");
    match builder.clone().with_native_roots() {
        Ok(builder) => builder.with_no_client_auth(),
        Err(err) => {
            warn!("no usable root certificates, so every TLS connection will fail: {err}");
            builder
                .with_root_certificates(rustls::RootCertStore::empty())
                .with_no_client_auth()
        }
    }
});

/// A timeout and the sleep impl that measures it.
///
/// NOTE: timeouts must go through the SDK's sleep impl, never `tokio::time`.
/// Persist configures marker durations that only its sleep impl translates
/// into real ones (see `mz_persist::s3`).
type Timeout = Option<(SharedAsyncSleep, Duration)>;

/// See the module documentation.
#[derive(Debug)]
pub(crate) struct Http1Client<R> {
    resolver: R,
    /// One connector, and so one connection pool, per pair of connect and read
    /// timeouts, as in the SDK's client.
    connectors: Mutex<BTreeMap<(Option<Duration>, Option<Duration>), SharedHttpConnector>>,
}

impl<R> Http1Client<R> {
    pub(crate) fn new(resolver: R) -> Self {
        Http1Client {
            resolver,
            connectors: Mutex::new(BTreeMap::new()),
        }
    }
}

impl<R> HttpClient for Http1Client<R>
where
    R: Service<Name> + Clone + Send + Sync + std::fmt::Debug + 'static,
    R::Response: Iterator<Item = SocketAddr>,
    R::Future: Send,
    R::Error: Into<BoxError>,
{
    fn http_connector(
        &self,
        settings: &HttpConnectorSettings,
        components: &RuntimeComponents,
    ) -> SharedHttpConnector {
        let key = (settings.connect_timeout(), settings.read_timeout());
        let mut connectors = self.connectors.lock().expect("lock poisoned");
        let connector = connectors.entry(key).or_insert_with(|| {
            // Like the SDK's client, this keeps the sleep impl of the first
            // caller with these timeouts.
            let sleep = components.sleep_impl().or_else(default_async_sleep);
            let timeout = |duration: Option<Duration>| {
                duration.map(|duration| {
                    let sleep = sleep.clone().expect("timeouts require a sleep impl");
                    (sleep, duration)
                })
            };
            SharedHttpConnector::new(Http1Connector::new(
                self.resolver.clone(),
                timeout(settings.connect_timeout()),
                timeout(settings.read_timeout()),
            ))
        });
        connector.clone()
    }

    fn validate_base_client_config(
        &self,
        _: &RuntimeComponentsBuilder,
        _: &ConfigBag,
    ) -> Result<(), BoxError> {
        // Loads the root certificates when the SDK client is built, instead of
        // during its first request.
        LazyLock::force(&TLS_CONFIG);
        Ok(())
    }

    fn connector_metadata(&self) -> Option<ConnectorMetadata> {
        Some(ConnectorMetadata::new("hyper", Some(Cow::Borrowed("1.x"))))
    }
}

#[derive(Debug)]
struct Http1Connector<R> {
    client: Client<ConnectTimeout<R>, SdkBody>,
    /// Covers everything from handing the request to the pool until the
    /// response head arrives, including waiting for a connection.
    read_timeout: Timeout,
}

impl<R> Http1Connector<R>
where
    R: Service<Name> + Clone + Send + Sync + 'static,
    R::Response: Iterator<Item = SocketAddr>,
    R::Future: Send,
    R::Error: Into<BoxError>,
{
    fn new(resolver: R, connect_timeout: Timeout, read_timeout: Timeout) -> Self {
        let mut tcp = TcpConnector::new_with_resolver(resolver);
        tcp.enforce_http(false);
        tcp.set_nodelay(true);
        let https = hyper_rustls::HttpsConnectorBuilder::new()
            .with_tls_config(TLS_CONFIG.clone())
            .https_or_http()
            .enable_http1()
            .wrap_connector(tcp);
        let mut builder = Client::builder(TokioExecutor::new());
        // The pool's idle timeout does nothing without a timer.
        builder.pool_timer(TokioTimer::new());
        Http1Connector {
            client: builder.build(ConnectTimeout {
                inner: https,
                timeout: connect_timeout,
            }),
            read_timeout,
        }
    }
}

impl<R> HttpConnector for Http1Connector<R>
where
    R: Service<Name> + Clone + Send + Sync + std::fmt::Debug + 'static,
    R::Response: Iterator<Item = SocketAddr>,
    R::Future: Send,
    R::Error: Into<BoxError>,
{
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        let mut request = match request.try_into_http1x() {
            Ok(request) => request,
            Err(err) => return HttpConnectorFuture::ready(Err(ConnectorError::user(err.into()))),
        };
        // Lets the SDK poison the connection after a timeout or transient
        // error, so the pool doesn't hand it to another request.
        let captured = capture_connection(&mut request);
        if let Some(capture) = request.extensions().get::<CaptureSmithyConnection>() {
            capture.set_connection_retriever(move || connection_metadata(&captured));
        }
        let response = self.client.request(request);
        let read_timeout = self.read_timeout.clone();
        HttpConnectorFuture::new(async move {
            let response = match read_timeout {
                Some((sleep, duration)) => tokio::select! {
                    response = response => response,
                    () = sleep.sleep(duration) => {
                        return Err(ConnectorError::timeout(Box::new(TimeoutError("read"))));
                    }
                },
                None => response.await,
            };
            let response = response.map_err(|err| classify(&err)(err.into()))?;
            HttpResponse::try_from(response.map(SdkBody::from_body_1_x))
                .map_err(|err| ConnectorError::other(err.into(), None))
        })
    }
}

/// Wraps the TLS connector to time out connection setup.
#[derive(Clone, Debug)]
struct ConnectTimeout<R> {
    inner: HttpsConnector<TcpConnector<R>>,
    timeout: Timeout,
}

impl<R> Service<Uri> for ConnectTimeout<R>
where
    R: Service<Name> + Clone + Send + Sync + 'static,
    R::Response: Iterator<Item = SocketAddr>,
    R::Future: Send,
    R::Error: Into<BoxError>,
{
    type Response = MaybeHttpsStream<TokioIo<TcpStream>>;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, BoxError>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), BoxError>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        let connecting = self.inner.call(uri);
        let timeout = self.timeout.clone();
        Box::pin(async move {
            match timeout {
                Some((sleep, duration)) => tokio::select! {
                    conn = connecting => conn,
                    () = sleep.sleep(duration) => Err(Box::new(TimeoutError("connect")).into()),
                },
                None => connecting.await,
            }
        })
    }
}

/// Leaves out the duration, which for persist is a marker value.
#[derive(Debug, thiserror::Error)]
#[error("HTTP {0} timeout")]
struct TimeoutError(&'static str);

fn connection_metadata(captured: &CaptureConnection) -> Option<ConnectionMetadata> {
    let connected = captured.connection_metadata();
    let connected = connected.as_ref()?;
    let mut extensions = Extensions::new();
    connected.get_extras(&mut extensions);
    let info = extensions.get::<HttpInfo>();
    let captured = captured.clone();
    let mut builder = ConnectionMetadata::builder()
        .proxied(connected.is_proxied())
        .poison_fn(move || {
            if let Some(connected) = captured.connection_metadata().as_ref() {
                connected.poison();
            }
        });
    builder
        .set_local_addr(info.map(HttpInfo::local_addr))
        .set_remote_addr(info.map(HttpInfo::remote_addr));
    Some(builder.build())
}

/// Picks the SDK error kind for a failed request, which decides whether the SDK
/// retries it. Matches the SDK client's classification minus its HTTP/2 cases.
fn classify(err: &(dyn Error + 'static)) -> fn(BoxError) -> ConnectorError {
    if find_source::<TimeoutError>(err).is_some() {
        return ConnectorError::timeout;
    }
    if let Some(hyper_err) = find_source::<hyper::Error>(err) {
        if hyper_err.is_timeout() {
            return ConnectorError::timeout;
        }
        if hyper_err.is_user() {
            return ConnectorError::user;
        }
        if hyper_err.is_closed()
            || hyper_err.is_canceled()
            || find_source::<std::io::Error>(hyper_err).is_some()
        {
            return ConnectorError::io;
        }
        // S3 sometimes cuts responses short.
        if hyper_err.is_incomplete_message() {
            return |err| ConnectorError::other(err, Some(ErrorKind::TransientError));
        }
        warn!(err = %DisplayErrorContext(err), "unrecognized error from hyper, not retrying");
        return |err| ConnectorError::other(err, None);
    }
    if let Some(hyper_util_err) = find_source::<hyper_util::client::legacy::Error>(err) {
        if hyper_util_err.is_connect() || find_source::<std::io::Error>(hyper_util_err).is_some() {
            return ConnectorError::io;
        }
    }
    |err| ConnectorError::other(err, None)
}

fn find_source<'a, E: Error + 'static>(err: &'a (dyn Error + 'static)) -> Option<&'a E> {
    std::iter::successors(Some(err), |&err| err.source()).find_map(|err| err.downcast_ref())
}
