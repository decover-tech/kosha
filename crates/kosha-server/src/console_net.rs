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
//! The address rules mirror the export upload guard (#347,
//! `exports::is_public_ip`); the two are to be merged into one.

use std::net::{IpAddr, Ipv4Addr, SocketAddr, ToSocketAddrs};

/// Whether `ip` is a public unicast address: not loopback, private,
/// link-local, CGNAT, multicast, unspecified, broadcast, documentation,
/// benchmarking, reserved, unique-local or site-local, and, for an IPv6
/// address that embeds an IPv4 one (IPv4-mapped, IPv4-compatible, NAT64
/// `64:ff9b::/96` and `64:ff9b:1::/48`, 6to4 `2002::/16`, Teredo
/// `2001::/32`), not one whose embedded address fails these rules.
/// Discard-only `100::/64` is refused.
pub(crate) fn is_public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_public_v4(v4),
        IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4_mapped() {
                return is_public_v4(v4);
            }
            let s = v6.segments();
            let v4_at = |hi: u16, lo: u16| {
                let [a, b] = hi.to_be_bytes();
                let [c, d] = lo.to_be_bytes();
                Ipv4Addr::new(a, b, c, d)
            };
            // NAT64, well-known (64:ff9b::/96) and local-use (64:ff9b:1::/48).
            if s[..6] == [0x64, 0xff9b, 0, 0, 0, 0] {
                return is_public_v4(v4_at(s[6], s[7]));
            }
            if s[0] == 0x64 && s[1] == 0xff9b && s[2] == 1 {
                return false;
            }
            // 6to4: the IPv4 address follows the prefix.
            if s[0] == 0x2002 {
                return is_public_v4(v4_at(s[1], s[2]));
            }
            // Teredo: the client's IPv4 address is the last 32 bits,
            // inverted; the server's is bits 32-63. Refuse when either is
            // not public.
            if s[0] == 0x2001 && s[1] == 0 {
                let client = v4_at(!s[6], !s[7]);
                let server = v4_at(s[2], s[3]);
                return is_public_v4(client) && is_public_v4(server);
            }
            !(v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_multicast()
                // ::/96 IPv4-compatible (deprecated)
                || s[..6] == [0, 0, 0, 0, 0, 0]
                // 100::/64 discard-only
                || s[..4] == [0x100, 0, 0, 0]
                // fc00::/7 unique local
                || (s[0] & 0xfe00) == 0xfc00
                // fe80::/10 link-local, fec0::/10 site-local
                || (s[0] & 0xffc0) == 0xfe80
                || (s[0] & 0xffc0) == 0xfec0
                // 2001:db8::/32 documentation
                || (s[0] == 0x2001 && s[1] == 0x0db8))
        }
    }
}

fn is_public_v4(v4: Ipv4Addr) -> bool {
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
        assert!(is_public_ip("2002:808:808::1".parse().unwrap()));
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
