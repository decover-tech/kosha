//! Outgoing email (#345): verification links, password resets and
//! organization invites from the console. Billing notices (dunning, #349)
//! can send through the same [`Mailer`].
//!
//! `KOSHA_MAILER` picks the implementation:
//! - `log` (the default): print each message to the server log. For
//!   development only: the log then holds the links in the messages, which
//!   are credentials until used. The server warns at startup when the
//!   console is on with this mailer.
//! - `smtp`: deliver through an SMTP relay (`KOSHA_SMTP_*`), over
//!   STARTTLS (default, port 587) or implicit TLS (`KOSHA_SMTP_TLS=tls`,
//!   port 465), verified against the operating system's trust store.
//!   `KOSHA_SMTP_TLS=none` sends in the clear and is meant for a relay on
//!   the same host; credentials are then refused, never sent in the clear.
//!
//! The SMTP client is deliberately small: one message per connection,
//! `AUTH PLAIN`, plain-text UTF-8 bodies. Every header value is checked for
//! CR/LF so a user-supplied address or name cannot inject headers.

use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use tokio_rustls::rustls;

/// Sends plain-text email. Implementations must be safe to call from any
/// request thread.
pub trait Mailer: Send + Sync {
    /// Send one message to one recipient.
    fn send(&self, to: &str, subject: &str, text_body: &str) -> Result<(), String>;
}

/// Which mailer `KOSHA_MAILER` selects, with a description for the startup
/// log.
pub fn from_env() -> Result<(Arc<dyn Mailer>, String), String> {
    let kind = std::env::var("KOSHA_MAILER").unwrap_or_default();
    match kind.trim().to_ascii_lowercase().as_str() {
        "" | "log" => Ok((Arc::new(LogMailer), "log (development only)".into())),
        "smtp" => {
            let mailer = SmtpMailer::from_env()?;
            let description = format!("smtp {}:{} ({:?})", mailer.host, mailer.port, mailer.tls);
            Ok((Arc::new(mailer), description))
        }
        other => Err(format!(
            "invalid KOSHA_MAILER '{other}' (expected 'log' or 'smtp')"
        )),
    }
}

/// Prints messages to stdout. Development only (see the module docs).
pub struct LogMailer;

impl Mailer for LogMailer {
    fn send(&self, to: &str, subject: &str, text_body: &str) -> Result<(), String> {
        println!(
            "mail (KOSHA_MAILER=log) to={to} subject={subject:?}\n{text_body}\n-- end of mail"
        );
        Ok(())
    }
}

/// Keeps every message in memory, for tests.
#[cfg(test)]
#[derive(Default)]
pub struct CapturingMailer {
    sent: std::sync::Mutex<Vec<(String, String, String)>>,
}

#[cfg(test)]
impl CapturingMailer {
    /// `(to, subject, body)` of every message so far.
    pub fn sent(&self) -> Vec<(String, String, String)> {
        self.sent.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

#[cfg(test)]
impl Mailer for CapturingMailer {
    fn send(&self, to: &str, subject: &str, text_body: &str) -> Result<(), String> {
        self.sent.lock().unwrap_or_else(|e| e.into_inner()).push((
            to.to_string(),
            subject.to_string(),
            text_body.to_string(),
        ));
        Ok(())
    }
}

/// How the SMTP connection is secured.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SmtpTls {
    /// Plain connection upgraded with `STARTTLS` (required).
    StartTls,
    /// TLS from the first byte (SMTPS).
    Implicit,
    /// No TLS. Credentials are refused in this mode.
    None,
}

pub struct SmtpMailer {
    host: String,
    port: u16,
    tls: SmtpTls,
    credentials: Option<(String, String)>,
    from: String,
    timeout: Duration,
    tls_config: Arc<rustls::ClientConfig>,
}

impl SmtpMailer {
    pub fn from_env() -> Result<Self, String> {
        let var = |name: &str| std::env::var(name).ok().filter(|v| !v.trim().is_empty());
        let host = var("KOSHA_SMTP_HOST").ok_or("KOSHA_MAILER=smtp needs KOSHA_SMTP_HOST")?;
        let tls = match var("KOSHA_SMTP_TLS")
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str()
        {
            "" | "starttls" => SmtpTls::StartTls,
            "tls" | "implicit" => SmtpTls::Implicit,
            "none" => SmtpTls::None,
            other => {
                return Err(format!(
                    "invalid KOSHA_SMTP_TLS '{other}' (expected starttls, tls or none)"
                ))
            }
        };
        let port = match var("KOSHA_SMTP_PORT") {
            Some(p) => p
                .trim()
                .parse()
                .map_err(|_| format!("invalid KOSHA_SMTP_PORT '{p}'"))?,
            None if tls == SmtpTls::Implicit => 465,
            None => 587,
        };
        let from = var("KOSHA_MAIL_FROM").ok_or("KOSHA_MAILER=smtp needs KOSHA_MAIL_FROM")?;
        let credentials = match (var("KOSHA_SMTP_USERNAME"), var("KOSHA_SMTP_PASSWORD")) {
            (Some(u), Some(p)) => Some((u, p)),
            (None, None) => None,
            _ => {
                return Err(
                    "set both KOSHA_SMTP_USERNAME and KOSHA_SMTP_PASSWORD, or neither".into(),
                )
            }
        };
        let tls_config = platform_tls_config()?;
        Self::new(host, port, tls, credentials, from, tls_config)
    }

    pub fn new(
        host: String,
        port: u16,
        tls: SmtpTls,
        credentials: Option<(String, String)>,
        from: String,
        tls_config: Arc<rustls::ClientConfig>,
    ) -> Result<Self, String> {
        check_address(&from).map_err(|e| format!("KOSHA_MAIL_FROM: {e}"))?;
        if tls == SmtpTls::None && credentials.is_some() {
            return Err(
                "KOSHA_SMTP_TLS=none cannot be used with SMTP credentials (they would be \
                 sent in the clear)"
                    .into(),
            );
        }
        Ok(Self {
            host,
            port,
            tls,
            credentials,
            from,
            timeout: Duration::from_secs(15),
            tls_config,
        })
    }

    fn connect(&self) -> Result<Conn, String> {
        let addr = (self.host.as_str(), self.port)
            .to_socket_addrs()
            .map_err(|e| format!("resolve {}: {e}", self.host))?
            .next()
            .ok_or_else(|| format!("resolve {}: no address", self.host))?;
        let tcp = TcpStream::connect_timeout(&addr, self.timeout)
            .map_err(|e| format!("connect {addr}: {e}"))?;
        tcp.set_read_timeout(Some(self.timeout)).ok();
        tcp.set_write_timeout(Some(self.timeout)).ok();
        match self.tls {
            SmtpTls::Implicit => self.wrap(tcp),
            _ => Ok(Conn::Plain(tcp)),
        }
    }

    fn wrap(&self, tcp: TcpStream) -> Result<Conn, String> {
        let name = rustls::pki_types::ServerName::try_from(self.host.clone())
            .map_err(|e| format!("invalid SMTP host name: {e}"))?;
        let conn = rustls::ClientConnection::new(self.tls_config.clone(), name)
            .map_err(|e| format!("tls: {e}"))?;
        Ok(Conn::Tls(Box::new(rustls::StreamOwned::new(conn, tcp))))
    }

    fn deliver(&self, to: &str, message: &str) -> Result<(), String> {
        let mut conn = self.connect()?;
        expect(&mut conn, 220)?;
        let ehlo = "EHLO kosha\r\n";
        command(&mut conn, ehlo, 250)?;
        if self.tls == SmtpTls::StartTls {
            command(&mut conn, "STARTTLS\r\n", 220)?;
            let Conn::Plain(tcp) = conn else {
                return Err("STARTTLS on a TLS connection".into());
            };
            conn = self.wrap(tcp)?;
            command(&mut conn, ehlo, 250)?;
        }
        if let Some((user, password)) = &self.credentials {
            let plain =
                base64::engine::general_purpose::STANDARD.encode(format!("\0{user}\0{password}"));
            command(&mut conn, &format!("AUTH PLAIN {plain}\r\n"), 235)
                .map_err(|_| "SMTP authentication failed".to_string())?;
        }
        command(&mut conn, &format!("MAIL FROM:<{}>\r\n", self.from), 250)?;
        command(&mut conn, &format!("RCPT TO:<{to}>\r\n"), 250)?;
        command(&mut conn, "DATA\r\n", 354)?;
        conn.write_all(message.as_bytes())
            .map_err(|e| format!("write: {e}"))?;
        command(&mut conn, "\r\n.\r\n", 250)?;
        // The message is accepted; a failed QUIT changes nothing.
        let _ = conn.write_all(b"QUIT\r\n");
        Ok(())
    }
}

impl Mailer for SmtpMailer {
    fn send(&self, to: &str, subject: &str, text_body: &str) -> Result<(), String> {
        check_address(to)?;
        let message = format_message(&self.from, to, subject, text_body)?;
        self.deliver(to, &message)
    }
}

/// A TLS client config trusting the operating system's roots, on the same
/// crypto provider as the rest of the server.
pub fn platform_tls_config() -> Result<Arc<rustls::ClientConfig>, String> {
    use rustls_platform_verifier::BuilderVerifierExt;
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| format!("tls: {e}"))?
        .with_platform_verifier()
        .map_err(|e| format!("tls: {e}"))?
        .with_no_client_auth();
    Ok(Arc::new(config))
}

enum Conn {
    Plain(TcpStream),
    Tls(Box<rustls::StreamOwned<rustls::ClientConnection, TcpStream>>),
}

impl Read for Conn {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Conn::Plain(s) => s.read(buf),
            Conn::Tls(s) => s.read(buf),
        }
    }
}

impl Write for Conn {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Conn::Plain(s) => s.write(buf),
            Conn::Tls(s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Conn::Plain(s) => s.flush(),
            Conn::Tls(s) => s.flush(),
        }
    }
}

/// Read one (possibly multi-line) reply; its code.
fn read_reply(conn: &mut Conn) -> Result<(u16, String), String> {
    let mut text = String::new();
    loop {
        let mut line = Vec::new();
        let mut byte = [0u8; 1];
        while !line.ends_with(b"\r\n") {
            if line.len() > 4096 {
                return Err("SMTP reply line too long".into());
            }
            match conn.read(&mut byte) {
                Ok(0) => return Err("SMTP server closed the connection".into()),
                Ok(_) => line.push(byte[0]),
                Err(e) => return Err(format!("read: {e}")),
            }
        }
        let line = String::from_utf8_lossy(&line).trim_end().to_string();
        let code: u16 = line
            .get(..3)
            .and_then(|c| c.parse().ok())
            .ok_or_else(|| format!("malformed SMTP reply '{line}'"))?;
        text.push_str(line.get(4..).unwrap_or(""));
        // "250-..." continues; "250 ..." (or a bare "250") ends the reply.
        if line.as_bytes().get(3) != Some(&b'-') {
            return Ok((code, text));
        }
        text.push('\n');
    }
}

fn expect(conn: &mut Conn, code: u16) -> Result<(), String> {
    let (got, text) = read_reply(conn)?;
    if got == code {
        Ok(())
    } else {
        Err(format!("SMTP: expected {code}, got {got} {text}"))
    }
}

fn command(conn: &mut Conn, line: &str, code: u16) -> Result<(), String> {
    conn.write_all(line.as_bytes())
        .and_then(|()| conn.flush())
        .map_err(|e| format!("write: {e}"))?;
    expect(conn, code)
}

/// An address fit for an SMTP envelope and a header: `local@domain`, no
/// spaces, angle brackets or line breaks.
fn check_address(address: &str) -> Result<(), String> {
    let ok = address.len() <= 254
        && address
            .split_once('@')
            .is_some_and(|(l, d)| !l.is_empty() && !d.is_empty())
        && !address
            .chars()
            .any(|c| c.is_whitespace() || c.is_control() || matches!(c, '<' | '>' | ',' | ';'));
    if ok {
        Ok(())
    } else {
        Err(format!("invalid email address '{address}'"))
    }
}

/// A header value: printable ASCII as is, anything else as an RFC 2047
/// encoded word. Line breaks are refused (header injection).
fn header_value(value: &str) -> Result<String, String> {
    if value.contains(['\r', '\n']) {
        return Err("line break in a mail header".into());
    }
    if value.bytes().all(|b| (0x20..0x7f).contains(&b)) {
        Ok(value.to_string())
    } else {
        Ok(format!(
            "=?UTF-8?B?{}?=",
            base64::engine::general_purpose::STANDARD.encode(value)
        ))
    }
}

/// The message as sent after `DATA`: headers, CRLF line endings, lines
/// starting with `.` doubled (RFC 5321 §4.5.2).
pub(crate) fn format_message(
    from: &str,
    to: &str,
    subject: &str,
    body: &str,
) -> Result<String, String> {
    let date = httpdate(std::time::SystemTime::now());
    let id = crate::console_crypto::random_hex(16);
    let domain = from.rsplit_once('@').map_or("kosha", |(_, d)| d);
    let mut message = format!(
        "From: {}\r\nTo: {}\r\nSubject: {}\r\nDate: {date}\r\nMessage-ID: <{id}@{domain}>\r\n\
         MIME-Version: 1.0\r\nContent-Type: text/plain; charset=utf-8\r\n\
         Content-Transfer-Encoding: 8bit\r\n\r\n",
        header_value(from)?,
        header_value(to)?,
        header_value(subject)?,
    );
    for line in body.replace("\r\n", "\n").split('\n') {
        if line.starts_with('.') {
            message.push('.');
        }
        message.push_str(line);
        message.push_str("\r\n");
    }
    // `deliver` sends the terminating "\r\n.\r\n" (the body already ends
    // with a line break, so the extra blank line is harmless).
    Ok(message)
}

/// RFC 5322 date in UTC, e.g. `Mon, 23 Nov 2026 10:00:00 +0000`.
fn httpdate(t: std::time::SystemTime) -> String {
    let secs = t
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as i64);
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    // Civil from days (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    const WEEKDAYS: [&str; 7] = ["Thu", "Fri", "Sat", "Sun", "Mon", "Tue", "Wed"];
    format!(
        "{}, {day:02} {} {year} {:02}:{:02}:{:02} +0000",
        WEEKDAYS[days.rem_euclid(7) as usize],
        MONTHS[(month - 1) as usize],
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader};
    use std::net::TcpListener;

    #[test]
    fn dates_are_rfc5322() {
        let t = std::time::UNIX_EPOCH + Duration::from_secs(1_795_428_000);
        assert_eq!(httpdate(t), "Mon, 23 Nov 2026 10:00:00 +0000");
        assert_eq!(
            httpdate(std::time::UNIX_EPOCH),
            "Thu, 01 Jan 1970 00:00:00 +0000"
        );
    }

    #[test]
    fn messages_are_dot_stuffed_and_refuse_header_injection() {
        let m = format_message(
            "noreply@kosha.dev",
            "ada@example.com",
            "Héllo",
            "line one\n.hidden\nend",
        )
        .unwrap();
        assert!(m.contains("Subject: =?UTF-8?B?"), "{m}");
        assert!(m.contains("\r\n..hidden\r\n"), "{m}");
        assert!(m.contains("Content-Type: text/plain; charset=utf-8\r\n"));
        assert!(format_message("a@b.c", "x@y.z", "hi\r\nBcc: evil@x.y", "b").is_err());
        assert!(check_address("ada@example.com").is_ok());
        for bad in ["ada", "a da@x.y", "a@x.y>\r\nRCPT TO:<b@x.y", "a@x.y,b@x.y"] {
            assert!(check_address(bad).is_err(), "{bad}");
        }
    }

    /// A one-message SMTP server: returns what it received.
    fn fake_smtp(listener: TcpListener) -> std::thread::JoinHandle<Vec<String>> {
        std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut out = stream;
            let mut seen = Vec::new();
            out.write_all(b"220 fake ESMTP\r\n").unwrap();
            let mut in_data = false;
            loop {
                let mut line = String::new();
                if reader.read_line(&mut line).unwrap() == 0 {
                    break;
                }
                seen.push(line.clone());
                if in_data {
                    if line == ".\r\n" {
                        in_data = false;
                        out.write_all(b"250 queued\r\n").unwrap();
                    }
                    continue;
                }
                let reply: &[u8] = match line.split_whitespace().next().unwrap_or("") {
                    "EHLO" => b"250-fake\r\n250 8BITMIME\r\n",
                    "DATA" => {
                        in_data = true;
                        b"354 go ahead\r\n"
                    }
                    "QUIT" => {
                        out.write_all(b"221 bye\r\n").unwrap();
                        break;
                    }
                    _ => b"250 ok\r\n",
                };
                out.write_all(reply).unwrap();
            }
            seen
        })
    }

    #[test]
    fn smtp_dialog_delivers_one_message() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = fake_smtp(listener);
        let mailer = SmtpMailer::new(
            "127.0.0.1".into(),
            port,
            SmtpTls::None,
            None,
            "noreply@kosha.dev".into(),
            platform_tls_config().unwrap(),
        )
        .unwrap();
        mailer
            .send("ada@example.com", "Verify", "Open this link:\nhttps://x/y")
            .unwrap();
        let seen = server.join().unwrap();
        assert_eq!(seen[0], "EHLO kosha\r\n");
        assert!(seen.contains(&"MAIL FROM:<noreply@kosha.dev>\r\n".to_string()));
        assert!(seen.contains(&"RCPT TO:<ada@example.com>\r\n".to_string()));
        assert!(seen.contains(&"Subject: Verify\r\n".to_string()));
        assert!(seen.contains(&"https://x/y\r\n".to_string()));
        assert!(seen.contains(&".\r\n".to_string()));
    }

    #[test]
    fn credentials_are_never_sent_without_tls() {
        let config = platform_tls_config().unwrap();
        let refused = SmtpMailer::new(
            "127.0.0.1".into(),
            25,
            SmtpTls::None,
            Some(("u".into(), "p".into())),
            "noreply@kosha.dev".into(),
            config.clone(),
        );
        assert!(refused.is_err());
    }

    /// STARTTLS: a server that does not offer it fails the send instead
    /// of falling back to plain text.
    #[test]
    fn starttls_is_required_when_configured() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            stream.write_all(b"220 fake\r\n").unwrap();
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            stream.write_all(b"250 fake\r\n").unwrap();
            line.clear();
            reader.read_line(&mut line).unwrap();
            stream.write_all(b"454 TLS not available\r\n").unwrap();
            line
        });
        let mailer = SmtpMailer::new(
            "127.0.0.1".into(),
            port,
            SmtpTls::StartTls,
            Some(("u".into(), "p".into())),
            "noreply@kosha.dev".into(),
            platform_tls_config().unwrap(),
        )
        .unwrap();
        let err = mailer.send("ada@example.com", "s", "b").unwrap_err();
        assert!(err.contains("454"), "{err}");
        assert_eq!(server.join().unwrap(), "STARTTLS\r\n");
    }
}
