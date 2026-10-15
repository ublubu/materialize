// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::net::SocketAddr;

use aws_config::{BehaviorVersion, ConfigLoader};
use aws_smithy_runtime_api::box_error::BoxError;
use aws_smithy_runtime_api::client::http::{HttpClient, SharedHttpClient};
use hyper_util::client::legacy::connect::dns::{GaiResolver, Name};
use tower::ServiceExt;
use tower::service_fn;
use tower::util::BoxCloneSyncService;

use crate::http_client::{Http1Client, Resolver};

mod http_client;
#[cfg(feature = "s3")]
pub mod s3;
#[cfg(feature = "s3")]
pub mod s3_uploader;

/// Creates an AWS SDK configuration loader with the defaults for the latest
/// behavior version plus some Materialize-specific overrides.
pub fn defaults() -> ConfigLoader {
    // Use the SDK's latest behavior version. We already pin the crate versions,
    // and CI puts version upgrades through rigorous testing, so we're happy to
    // take the latest behavior version. We can adjust this in the future as
    // necessary, if the AWS SDK ships a new behavior version that causes
    // trouble.
    let behavior_version = BehaviorVersion::latest();

    // This is the only method allowed to call `aws_config::defaults`.
    #[allow(clippy::disallowed_methods)]
    let loader = aws_config::defaults(behavior_version);

    // Install our custom HTTP client.
    let loader = loader.http_client(http_client());

    loader
}

/// Returns an HTTP client for use with the AWS SDK that is appropriately
/// configured for Materialize. It speaks only HTTP/1.1.
pub fn http_client() -> impl HttpClient {
    let resolver = GaiResolver::new()
        .map_response(|addrs| addrs.collect::<Vec<_>>().into_iter())
        .map_err(BoxError::from);
    Http1Client::new(BoxCloneSyncService::new(resolver))
}

/// Returns an AWS SDK HTTP client, like [`http_client`], whose DNS resolver
/// delegates to [`mz_ore::netio::resolve_address`].
///
/// Only the IP resolution step is overridden. The client still uses the
/// original hostname for SNI and TLS certificate validation, so HTTPS endpoints
/// work unchanged.
pub fn http_client_with_resolver(enforce_external_addresses: bool) -> SharedHttpClient {
    SharedHttpClient::new(Http1Client::new(mz_resolver(enforce_external_addresses)))
}

/// A DNS resolver that delegates to [`mz_ore::netio::resolve_address`].
fn mz_resolver(enforce_external_addresses: bool) -> Resolver {
    BoxCloneSyncService::new(service_fn(move |name: Name| async move {
        let ips = mz_ore::netio::resolve_address(name.as_str(), enforce_external_addresses).await?;
        // hyper-util replaces port 0 with the request's port.
        let addrs: Vec<_> = ips.into_iter().map(|ip| SocketAddr::new(ip, 0)).collect();
        Ok::<_, BoxError>(addrs.into_iter())
    }))
}

#[cfg(test)]
mod tests {
    use std::net::IpAddr;
    use std::str::FromStr;

    use mz_ore::netio::DnsResolutionError;

    use super::*;

    async fn resolve(
        enforce_external_addresses: bool,
        host: &str,
    ) -> Result<Vec<IpAddr>, BoxError> {
        let name = Name::from_str(host).expect("valid name");
        let addrs = mz_resolver(enforce_external_addresses)
            .oneshot(name)
            .await?;
        Ok(addrs.map(|addr| addr.ip()).collect())
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn resolver_rejects_loopback_when_enforced() {
        let err = resolve(true, "127.0.0.1")
            .await
            .expect_err("must reject loopback");
        assert!(
            matches!(
                err.downcast_ref::<DnsResolutionError>(),
                Some(DnsResolutionError::PrivateAddress)
            ),
            "got {err:?}"
        );
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn resolver_allows_loopback_when_not_enforced() {
        let addrs = resolve(false, "127.0.0.1")
            .await
            .expect("loopback should resolve when enforcement is off");
        assert!(addrs.contains(&IpAddr::from([127, 0, 0, 1])));
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn resolver_allows_public_when_enforced() {
        let addrs = resolve(true, "8.8.8.8")
            .await
            .expect("public IP should resolve");
        assert!(addrs.contains(&IpAddr::from([8, 8, 8, 8])));
    }
}
