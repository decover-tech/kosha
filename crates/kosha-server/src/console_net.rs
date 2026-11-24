//! Where the console's identity-provider client may connect (#345).
//!
//! An organization's admins choose its OIDC issuer, and the server then
//! fetches the issuer's discovery document, JWKS and token endpoint. So
//! every such URL must be `https`, carry no credentials, and resolve only
//! to public unicast addresses; the request then connects to the address
//! that was checked (no second DNS lookup), through no proxy, following no
//! redirects. Without this an admin could point "SSO" at the server's own
//! network (metadata services, internal admin ports).
//!
//! The address rules are the export upload guard's
//! (`public_url::is_public_ip`), so the two cannot drift apart.

use std::net::{SocketAddr, ToSocketAddrs};

/// The shared outbound-address guard (#347's `public_url`): one rule set
/// for every URL the server fetches on a customer's or admin's say-so.
pub(crate) use crate::public_url::is_public_ip;

/// A URL that passed [`check_url`], with the address to connect to.
#[derive(Debug, Clone)]
pub(crate) struct Target {
    pub url: reqwest::Url,
    pub host: String,
    pub addr: SocketAddr,
}

/// Check a URL the identity-provider client is about to fetch. The error
/// names the URL and why; it is for the log, not for the caller.
/// `allow_insecure` (tests only) admits `http` and loopback.
pub(crate) fn check_url(raw: &str, allow_insecure: bool) -> Result<Target, String> {
    let url = reqwest::Url::parse(raw).map_err(|e| format!("{raw:?} is not a URL: {e}"))?;
    match url.scheme() {
        "https" => {}
        "http" if allow_insecure => {}
        other => return Err(format!("{raw:?}: must use https, not {other}")),
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(format!("{raw:?}: credentials in the URL"));
    }
    let host = url
        .host_str()
        .filter(|h| !h.is_empty())
        .ok_or_else(|| format!("{raw:?}: no host"))?
        .to_string();
    let port = url
        .port_or_known_default()
        .ok_or_else(|| format!("{raw:?}: no port"))?;
    let lookup = host
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_string();
    let addrs: Vec<SocketAddr> = (lookup.as_str(), port)
        .to_socket_addrs()
        .map_err(|e| format!("{host:?} does not resolve: {e}"))?
        .collect();
    let first = *addrs
        .first()
        .ok_or_else(|| format!("{host:?} does not resolve"))?;
    for addr in &addrs {
        let ip = addr.ip();
        if !(is_public_ip(ip) || (allow_insecure && ip.is_loopback())) {
            return Err(format!("{host:?} resolves to {ip}, not a public address"));
        }
    }
    Ok(Target {
        url,
        host: lookup,
        addr: first,
    })
}

/// A client for one checked target: pinned to its address, no proxy, no
/// redirects, short timeouts.
pub(crate) fn client_for(target: &Target) -> Result<reqwest::blocking::Client, String> {
    reqwest::blocking::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .resolve(&target.host, target.addr)
        .timeout(std::time::Duration::from_secs(10))
        .connect_timeout(std::time::Duration::from_secs(5))
        .user_agent("kosha-console")
        .build()
        .map_err(|e| format!("HTTP client: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_public_addresses_pass() {
        for public in [
            "8.8.8.8",
            "1.1.1.1",
            "2606:4700:4700::1111",
            "64:ff9b::808:808",
        ] {
            assert!(is_public_ip(public.parse().unwrap()), "{public}");
        }
        for blocked in [
            "127.0.0.1",
            "10.0.0.1",
            "172.16.5.4",
            "192.168.1.1",
            "169.254.169.254",
            "100.64.0.1",
            "0.0.0.0",
            "224.0.0.1",
            "255.255.255.255",
            "::1",
            "::",
            "fe80::1",
            "fd00::1",
            "ff02::1",
            "::ffff:127.0.0.1",
            "::ffff:169.254.169.254",
            "64:ff9b::a9fe:a9fe",
            "64:ff9b:1::1",
            "2002:7f00:1::1",
            "2002:a9fe:a9fe::1",
            "2001:0:4136:e378:8000:63bf:80ff:fffe",
            "100::1",
        ] {
            assert!(!is_public_ip(blocked.parse().unwrap()), "{blocked}");
        }
        // 6to4 of a public address is public.
        // 6to4 is refused outright, even around a public IPv4 address: the
        // relay decides where the bytes go.
        assert!(!is_public_ip("2002:808:808::1".parse().unwrap()));
    }

    #[test]
    fn urls_must_be_https_and_public() {
        assert!(check_url("http://example.com/x", false).is_err());
        assert!(check_url("https://127.0.0.1/x", false).is_err());
        assert!(check_url("https://user:pw@example.com/", false).is_err());
        assert!(check_url("https://[::1]:8443/", false).is_err());
        assert!(check_url("http://127.0.0.1:1/x", true).is_ok());
    }
}
