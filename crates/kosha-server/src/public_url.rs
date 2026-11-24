//! Outbound URLs a tenant or an operator supplies (#347 export uploads;
//! reusable for e.g. SSO issuer URLs): only `https` to public addresses,
//! fetched without proxies or redirects, at the address that was checked.
//!
//! The guard against server-side request forgery is two parts:
//! [`validate_public_url`] parses the URL, resolves its host once and
//! refuses it unless every address is public ([`is_public_ip`]); and
//! [`pinned_client`] builds a blocking `reqwest` client that connects to
//! exactly the address that was checked (no second DNS lookup to rebind),
//! ignores `HTTP(S)_PROXY` from the environment (a proxy would resolve
//! the host itself, bypassing the pin) and follows no redirect (a redirect
//! could point anywhere).

use std::net::{IpAddr, Ipv4Addr, SocketAddr, ToSocketAddrs};
use std::time::Duration;

/// Upper bound on a URL's length.
const MAX_URL_LEN: usize = 8192;

/// A URL that passed [`validate_public_url`], with the address to connect
/// to.
#[derive(Debug, Clone)]
pub(crate) struct PublicUrl {
    pub url: reqwest::Url,
    /// The host as written, without IPv6 brackets.
    pub host: String,
    /// The checked address the connection must go to.
    pub addr: SocketAddr,
}

fn embedded_v4(high: u16, low: u16) -> Ipv4Addr {
    let [a, b] = high.to_be_bytes();
    let [c, d] = low.to_be_bytes();
    Ipv4Addr::new(a, b, c, d)
}

/// Whether `ip` is a public unicast address an outbound request may reach.
/// Refused: loopback, private, link-local, CGNAT, unspecified, broadcast,
/// multicast, reserved, benchmarking, documentation and IETF-protocol
/// ranges; for IPv6 also unique-local, site-local, discard-only
/// (`100::/64`), 6to4 (`2002::/16`), Teredo (`2001::/32`), local-use NAT64
/// (`64:ff9b:1::/48`), IPv4-compatible forms, and IPv4-mapped or NAT64
/// (`64:ff9b::/96`) forms of a refused IPv4 address. The tunnelling forms
/// are refused outright: their relays decide where the bytes go.
pub(crate) fn is_public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            let [a, b, c, _] = v4.octets();
            !(v4.is_private()
                || v4.is_loopback()
                || v4.is_link_local()
                || v4.is_unspecified()
                || v4.is_broadcast()
                || v4.is_multicast()
                || v4.is_documentation()
                || a == 0
                || a >= 240
                // 100.64.0.0/10 carrier-grade NAT
                || (a == 100 && (64..128).contains(&b))
                // 192.0.0.0/24 IETF protocol assignments
                || (a == 192 && b == 0 && c == 0)
                // 198.18.0.0/15 benchmarking
                || (a == 198 && (b == 18 || b == 19)))
        }
        IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4_mapped() {
                return is_public_ip(IpAddr::V4(v4));
            }
            let s = v6.segments();
            // NAT64 well-known prefix (64:ff9b::/96) reaches the embedded
            // IPv4 address.
            if s[..6] == [0x64, 0xff9b, 0, 0, 0, 0] {
                return is_public_ip(IpAddr::V4(embedded_v4(s[6], s[7])));
            }
            !(v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_multicast()
                // fc00::/7 unique local
                || (s[0] & 0xfe00) == 0xfc00
                // fe80::/10 link-local, fec0::/10 site-local
                || (s[0] & 0xffc0) == 0xfe80
                || (s[0] & 0xffc0) == 0xfec0
                // 2001:db8::/32 documentation
                || (s[0] == 0x2001 && s[1] == 0x0db8)
                // 2001::/32 Teredo
                || (s[0] == 0x2001 && s[1] == 0)
                // 2002::/16 6to4
                || s[0] == 0x2002
                // 64:ff9b:1::/48 local-use NAT64
                || (s[0] == 0x64 && s[1] == 0xff9b && s[2] == 1)
                // 100::/64 discard-only
                || s[..4] == [0x100, 0, 0, 0]
                // ::/96 IPv4-compatible (deprecated)
                || s[..6] == [0, 0, 0, 0, 0, 0])
        }
    }
}

/// Validate an outbound URL a caller supplied: `https` only, no
/// credentials before the host, and every address its host resolves to
/// public. `what` names the URL in errors (`"upload_url"`). The first
/// resolved address is the one [`pinned_client`] connects to.
/// `allow_insecure` (tests only, never from configuration) admits `http`
/// and loopback.
pub(crate) fn validate_public_url(
    raw: &str,
    what: &str,
    allow_insecure: bool,
) -> Result<PublicUrl, String> {
    if raw.len() > MAX_URL_LEN {
        return Err(format!("{what} is longer than {MAX_URL_LEN} bytes"));
    }
    let url = reqwest::Url::parse(raw).map_err(|e| format!("{what} is not a URL: {e}"))?;
    match url.scheme() {
        "https" => {}
        "http" if allow_insecure => {}
        other => return Err(format!("{what} must use https, not {other}")),
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(format!("{what} must not carry credentials before the host"));
    }
    let host = url
        .host_str()
        .filter(|h| !h.is_empty())
        .ok_or_else(|| format!("{what} has no host"))?
        .to_string();
    let port = url
        .port_or_known_default()
        .ok_or_else(|| format!("{what} has no port"))?;
    let lookup = host.trim_start_matches('[').trim_end_matches(']');
    let addrs: Vec<SocketAddr> = (lookup, port)
        .to_socket_addrs()
        .map_err(|e| format!("{what} host {host:?} does not resolve: {e}"))?
        .collect();
    let Some(first) = addrs.first().copied() else {
        return Err(format!("{what} host {host:?} does not resolve"));
    };
    for addr in &addrs {
        let ip = addr.ip();
        let allowed = is_public_ip(ip) || (allow_insecure && ip.is_loopback());
        if !allowed {
            return Err(format!(
                "{what} host {host:?} resolves to {ip}, which is not a public address"
            ));
        }
    }
    Ok(PublicUrl {
        url,
        host: lookup.to_string(),
        addr: first,
    })
}

/// A blocking client for requests to `target` only: connects to the
/// checked address, uses no proxy, follows no redirect.
pub(crate) fn pinned_client(
    target: &PublicUrl,
    timeout: Duration,
) -> Result<reqwest::blocking::Client, String> {
    reqwest::blocking::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .resolve(&target.host, target.addr)
        .timeout(timeout)
        .connect_timeout(Duration::from_secs(10))
        .build()
        .map_err(|e| format!("http client: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tunnelling_and_special_ipv6_ranges_are_not_public() {
        for raw in [
            "2002:0808:0808::1",   // 6to4 of a public address: still refused
            "2002:0a00:0001::1",   // 6to4 of 10.0.0.1
            "2001:0:4136:e378::1", // Teredo
            "64:ff9b:1::a00:1",    // local-use NAT64
            "100::1",              // discard-only
            "64:ff9b::a9fe:a9fe",  // NAT64 of 169.254.169.254
            "::ffff:127.0.0.1",
            "fec0::1",
        ] {
            let ip: IpAddr = raw.parse().unwrap();
            assert!(!is_public_ip(ip), "{raw}");
        }
        for raw in ["2606:4700::1111", "64:ff9b::808:808", "8.8.8.8"] {
            let ip: IpAddr = raw.parse().unwrap();
            assert!(is_public_ip(ip), "{raw}");
        }
    }

    #[test]
    fn the_name_of_the_url_is_in_every_error() {
        let error = validate_public_url("http://8.8.8.8/x", "issuer_url", false).unwrap_err();
        assert!(error.starts_with("issuer_url must use https"), "{error}");
        let error = validate_public_url("https://[2002::1]/x", "issuer_url", false).unwrap_err();
        assert!(error.contains("not a public address"), "{error}");
    }
}
