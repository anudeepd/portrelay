use std::{collections::HashSet, fmt, fs, path::PathBuf, str::FromStr, time::Duration};

use serde::{de::Error as _, Deserialize, Deserializer};

use crate::{cli::ServeArgs, error::ConfigError};

pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
pub const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);
pub const DEFAULT_BUFFER_SIZE: usize = 16 * 1024;
pub const DEFAULT_BUFFER_POOL_SIZE: usize = 8 * 1024;
pub const DEFAULT_CONNECTION_LOG_SAMPLE_RATE: usize = 100;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Endpoint {
    address: String,
    host: String,
    port: u16,
}

impl Endpoint {
    pub fn parse(
        value: &str,
        allow_empty_host: bool,
        allow_port_zero: bool,
    ) -> Result<Self, ConfigError> {
        let value = value.trim();
        if value.is_empty() {
            return Err(ConfigError::invalid("endpoint cannot be empty"));
        }

        let (host, port_text) = if let Some(rest) = value.strip_prefix('[') {
            let close = rest
                .find(']')
                .ok_or_else(|| ConfigError::invalid(format!("invalid IPv6 endpoint {value:?}")))?;
            let host = &rest[..close];
            let suffix = &rest[close + 1..];
            let port = suffix.strip_prefix(':').ok_or_else(|| {
                ConfigError::invalid(format!("endpoint must include port: {value:?}"))
            })?;
            (host, port)
        } else {
            let split = value.rfind(':').ok_or_else(|| {
                ConfigError::invalid(format!("endpoint must include port: {value:?}"))
            })?;
            let host = &value[..split];
            let port = &value[split + 1..];
            if host.contains(':') {
                return Err(ConfigError::invalid(
                    "IPv6 endpoints must use bracket notation, for example [::1]:3306",
                ));
            }
            (host, port)
        };

        let port = port_text.parse::<u16>().map_err(|error| {
            ConfigError::invalid(format!("invalid TCP port {port_text:?}: {error}"))
        })?;
        if port == 0 && !allow_port_zero {
            return Err(ConfigError::invalid("TCP port must be greater than zero"));
        }

        let host = if host.is_empty() {
            if !allow_empty_host {
                return Err(ConfigError::invalid("target host cannot be empty"));
            }
            "0.0.0.0"
        } else {
            host
        };
        if host.chars().any(char::is_whitespace) || host == "*" {
            return Err(ConfigError::invalid(format!(
                "invalid endpoint host {host:?}"
            )));
        }

        let address = if host.contains(':') {
            format!("[{host}]:{port}")
        } else {
            format!("{host}:{port}")
        };
        Ok(Self {
            address,
            host: host.to_owned(),
            port,
        })
    }

    pub fn address(&self) -> &str {
        &self.address
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self) -> u16 {
        self.port
    }
}

impl fmt::Display for Endpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.address.fmt(formatter)
    }
}

impl FromStr for Endpoint {
    type Err = ConfigError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value, false, false)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ForwardMode {
    #[default]
    Auto,
    Tokio,
    Splice,
}

impl FromStr for ForwardMode {
    type Err = ConfigError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "auto" => Ok(Self::Auto),
            "tokio" | "copy" => Ok(Self::Tokio),
            "splice" => Ok(Self::Splice),
            other => Err(ConfigError::invalid(format!(
                "forward_mode must be one of auto, tokio, or splice, got {other:?}"
            ))),
        }
    }
}

#[derive(Clone, Debug)]
pub struct RelaySpec {
    pub name: String,
    pub listen: Endpoint,
    pub target: Endpoint,
    pub connect_timeout: Duration,
    pub idle_timeout: Option<Duration>,
    pub max_connections: Option<usize>,
    pub max_connecting: Option<usize>,
    pub tcp_keepalive: Option<Duration>,
    pub dns_refresh: Option<Duration>,
    pub socket_buffer_size: Option<usize>,
    pub buffer_size: usize,
    pub forward_mode: ForwardMode,
}

#[derive(Clone, Debug)]
pub struct Settings {
    pub log_level: String,
    pub metrics_listen: Option<Endpoint>,
    pub metrics_bearer_token: Option<String>,
    pub metrics_tls_cert: Option<PathBuf>,
    pub metrics_tls_key: Option<PathBuf>,
    pub shutdown_timeout: Duration,
    pub global_max_connections: Option<usize>,
    pub global_max_connecting: Option<usize>,
    pub buffer_pool_size: usize,
    pub connection_log_sample_rate: usize,
    pub reuse_port: bool,
    pub relays: Vec<RelaySpec>,
}

impl Settings {
    pub fn single(listen: &str, target: &str) -> Result<Self, ConfigError> {
        let relay = RelaySpec {
            name: "default".to_owned(),
            listen: Endpoint::parse(listen, true, true)?,
            target: Endpoint::parse(target, false, false)?,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            idle_timeout: None,
            max_connections: None,
            max_connecting: None,
            tcp_keepalive: None,
            dns_refresh: None,
            socket_buffer_size: None,
            buffer_size: DEFAULT_BUFFER_SIZE,
            forward_mode: ForwardMode::default(),
        };
        let settings = Self {
            log_level: "info".to_owned(),
            metrics_listen: None,
            metrics_bearer_token: None,
            metrics_tls_cert: None,
            metrics_tls_key: None,
            shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
            global_max_connections: None,
            global_max_connecting: None,
            buffer_pool_size: DEFAULT_BUFFER_POOL_SIZE,
            connection_log_sample_rate: DEFAULT_CONNECTION_LOG_SAMPLE_RATE,
            reuse_port: false,
            relays: vec![relay],
        };
        settings.validate()?;
        Ok(settings)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.relays.is_empty() {
            return Err(ConfigError::invalid(
                "at least one relay mapping is required",
            ));
        }
        if self.shutdown_timeout.is_zero() {
            return Err(ConfigError::invalid(
                "shutdown_timeout must be greater than zero",
            ));
        }
        validate_limit(self.global_max_connections, "global max_connections")?;
        validate_limit(self.global_max_connecting, "global max_connecting")?;
        if self.buffer_pool_size < 2 || self.buffer_pool_size % 2 != 0 {
            return Err(ConfigError::invalid(
                "buffer_pool_size must be an even number of at least 2",
            ));
        }
        if self.connection_log_sample_rate == 0 {
            return Err(ConfigError::invalid(
                "connection_log_sample_rate must be greater than zero",
            ));
        }
        if self.metrics_tls_cert.is_some() != self.metrics_tls_key.is_some() {
            return Err(ConfigError::invalid(
                "metrics_tls_cert and metrics_tls_key must be configured together",
            ));
        }
        if (self.metrics_tls_cert.is_some() || self.metrics_tls_key.is_some())
            && self.metrics_listen.is_none()
        {
            return Err(ConfigError::invalid("metrics TLS requires metrics_listen"));
        }
        if self
            .metrics_bearer_token
            .as_deref()
            .is_some_and(|token| token.is_empty())
        {
            return Err(ConfigError::invalid("metrics_bearer_token cannot be empty"));
        }
        #[cfg(not(unix))]
        if self.reuse_port {
            return Err(ConfigError::invalid(
                "reuse_port is only supported on Unix platforms",
            ));
        }

        let mut names = HashSet::with_capacity(self.relays.len());
        let mut listeners = HashSet::with_capacity(self.relays.len());
        for relay in &self.relays {
            if relay.name.trim().is_empty() {
                return Err(ConfigError::invalid("relay name cannot be empty"));
            }
            if !names.insert(relay.name.clone()) {
                return Err(ConfigError::invalid(format!(
                    "duplicate relay name {:?}",
                    relay.name
                )));
            }
            if relay.listen.port() != 0 && !listeners.insert(relay.listen.address().to_owned()) {
                return Err(ConfigError::invalid(format!(
                    "duplicate listener address {:?}",
                    relay.listen.address()
                )));
            }
            if relay.connect_timeout.is_zero() {
                return Err(ConfigError::invalid(format!(
                    "relay {:?} connect_timeout must be greater than zero",
                    relay.name
                )));
            }
            if relay.idle_timeout.is_some_and(|value| value.is_zero()) {
                return Err(ConfigError::invalid(format!(
                    "relay {:?} idle_timeout must be greater than zero",
                    relay.name
                )));
            }
            if relay.tcp_keepalive.is_some_and(|value| value.is_zero()) {
                return Err(ConfigError::invalid(format!(
                    "relay {:?} tcp_keepalive must be greater than zero",
                    relay.name
                )));
            }
            validate_limit(
                relay.max_connections,
                &format!("relay {:?} max_connections", relay.name),
            )?;
            validate_limit(
                relay.max_connecting,
                &format!("relay {:?} max_connecting", relay.name),
            )?;
            if relay.dns_refresh.is_some_and(|value| value.is_zero()) {
                return Err(ConfigError::invalid(format!(
                    "relay {:?} dns_refresh must be greater than zero",
                    relay.name
                )));
            }
            if relay.socket_buffer_size == Some(0) {
                return Err(ConfigError::invalid(format!(
                    "relay {:?} socket_buffer_size must be greater than zero",
                    relay.name
                )));
            }
            if relay.buffer_size == 0 {
                return Err(ConfigError::invalid(format!(
                    "relay {:?} buffer_size must be greater than zero",
                    relay.name
                )));
            }
            #[cfg(not(target_os = "linux"))]
            if relay.forward_mode == ForwardMode::Splice {
                return Err(ConfigError::invalid(
                    "forward_mode=splice is only supported on Linux",
                ));
            }
        }
        Ok(())
    }
}

fn validate_limit(value: Option<usize>, name: &str) -> Result<(), ConfigError> {
    if value == Some(0) {
        return Err(ConfigError::invalid(format!(
            "{name} must be greater than zero"
        )));
    }
    Ok(())
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    #[serde(default)]
    global: GlobalFileConfig,
    #[serde(default)]
    relay: Vec<RelayFileConfig>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct GlobalFileConfig {
    #[serde(default)]
    log_level: Option<String>,
    #[serde(default)]
    metrics_listen: Option<String>,
    #[serde(default)]
    metrics_bearer_token: Option<String>,
    #[serde(default)]
    metrics_tls_cert: Option<PathBuf>,
    #[serde(default)]
    metrics_tls_key: Option<PathBuf>,
    #[serde(default, deserialize_with = "deserialize_duration_opt")]
    connect_timeout: Option<Duration>,
    #[serde(default, deserialize_with = "deserialize_duration_opt")]
    idle_timeout: Option<Duration>,
    #[serde(default, deserialize_with = "deserialize_duration_opt")]
    shutdown_timeout: Option<Duration>,
    #[serde(default)]
    max_connections: Option<usize>,
    #[serde(default)]
    max_connecting: Option<usize>,
    #[serde(default, deserialize_with = "deserialize_duration_opt")]
    tcp_keepalive: Option<Duration>,
    #[serde(default, deserialize_with = "deserialize_duration_opt")]
    dns_refresh: Option<Duration>,
    #[serde(default)]
    socket_buffer_size: Option<usize>,
    #[serde(default)]
    buffer_size: Option<usize>,
    #[serde(default)]
    buffer_pool_size: Option<usize>,
    #[serde(default)]
    connection_log_sample_rate: Option<usize>,
    #[serde(default)]
    forward_mode: Option<String>,
    #[serde(default)]
    reuse_port: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RelayFileConfig {
    #[serde(default)]
    name: Option<String>,
    listen: String,
    target: String,
    #[serde(default, deserialize_with = "deserialize_duration_opt")]
    connect_timeout: Option<Duration>,
    #[serde(default, deserialize_with = "deserialize_duration_opt")]
    idle_timeout: Option<Duration>,
    #[serde(default)]
    max_connections: Option<usize>,
    #[serde(default)]
    max_connecting: Option<usize>,
    #[serde(default, deserialize_with = "deserialize_duration_opt")]
    tcp_keepalive: Option<Duration>,
    #[serde(default, deserialize_with = "deserialize_duration_opt")]
    dns_refresh: Option<Duration>,
    #[serde(default)]
    socket_buffer_size: Option<usize>,
    #[serde(default)]
    buffer_size: Option<usize>,
    #[serde(default)]
    forward_mode: Option<String>,
}
fn deserialize_duration_opt<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = Option::<String>::deserialize(deserializer)?;
    raw.map(|value| humantime::parse_duration(value.trim()).map_err(D::Error::custom))
        .transpose()
}

pub fn load_settings(args: &ServeArgs) -> Result<Settings, ConfigError> {
    if let Some(path) = &args.config {
        if args.connect_timeout.is_some()
            || args.idle_timeout.is_some()
            || args.max_connections.is_some()
            || args.max_connecting.is_some()
            || args.tcp_keepalive.is_some()
            || args.dns_refresh.is_some()
            || args.socket_buffer_size.is_some()
            || args.buffer_size.is_some()
            || args.buffer_pool_size.is_some()
            || args.connection_log_sample_rate.is_some()
            || args.forward_mode.is_some()
            || args.reuse_port
            || args.metrics_token.is_some()
            || args.metrics_tls_cert.is_some()
            || args.metrics_tls_key.is_some()
        {
            return Err(ConfigError::invalid(
                "relay runtime options must be set in TOML when --config is used",
            ));
        }
        let contents = fs::read_to_string(path).map_err(|source| ConfigError::ReadConfig {
            path: path.clone(),
            source,
        })?;
        let file = toml::from_str::<FileConfig>(&contents)?;
        settings_from_file(file, args)
    } else {
        settings_from_cli(args)
    }
}

fn settings_from_cli(args: &ServeArgs) -> Result<Settings, ConfigError> {
    let listen = args.listen.as_deref().ok_or_else(|| {
        ConfigError::invalid("--listen and --target are required without --config")
    })?;
    let target = args.target.as_deref().ok_or_else(|| {
        ConfigError::invalid("--listen and --target are required without --config")
    })?;
    let forward_mode = args
        .forward_mode
        .as_deref()
        .map(ForwardMode::from_str)
        .transpose()?
        .unwrap_or_default();

    let relay = RelaySpec {
        name: "default".to_owned(),
        listen: Endpoint::parse(listen, true, true)?,
        target: Endpoint::parse(target, false, false)?,
        connect_timeout: args.connect_timeout.unwrap_or(DEFAULT_CONNECT_TIMEOUT),
        idle_timeout: args.idle_timeout,
        max_connections: None,
        max_connecting: None,
        tcp_keepalive: args.tcp_keepalive,
        dns_refresh: args.dns_refresh,
        socket_buffer_size: args.socket_buffer_size,
        buffer_size: args.buffer_size.unwrap_or(DEFAULT_BUFFER_SIZE),
        forward_mode,
    };
    let settings = Settings {
        log_level: args.log_level.clone().unwrap_or_else(|| "info".to_owned()),
        metrics_listen: args
            .metrics_listen
            .as_deref()
            .map(|value| Endpoint::parse(value, true, true))
            .transpose()?,
        metrics_bearer_token: args.metrics_token.clone(),
        metrics_tls_cert: args.metrics_tls_cert.clone(),
        metrics_tls_key: args.metrics_tls_key.clone(),
        shutdown_timeout: args.shutdown_timeout.unwrap_or(DEFAULT_SHUTDOWN_TIMEOUT),
        global_max_connections: args.max_connections,
        global_max_connecting: args.max_connecting,
        buffer_pool_size: args.buffer_pool_size.unwrap_or(DEFAULT_BUFFER_POOL_SIZE),
        connection_log_sample_rate: args
            .connection_log_sample_rate
            .unwrap_or(DEFAULT_CONNECTION_LOG_SAMPLE_RATE),
        reuse_port: args.reuse_port,
        relays: vec![relay],
    };
    settings.validate()?;
    Ok(settings)
}

fn settings_from_file(file: FileConfig, args: &ServeArgs) -> Result<Settings, ConfigError> {
    if file.relay.is_empty() {
        return Err(ConfigError::invalid(
            "configuration must contain at least one [[relay]] entry",
        ));
    }

    let log_level = args
        .log_level
        .clone()
        .or(file.global.log_level)
        .unwrap_or_else(|| "info".to_owned());
    let metrics_listen = args
        .metrics_listen
        .as_deref()
        .map(|value| Endpoint::parse(value, true, true))
        .transpose()?
        .or(file
            .global
            .metrics_listen
            .as_deref()
            .map(|value| Endpoint::parse(value, true, true))
            .transpose()?);
    let metrics_bearer_token = file.global.metrics_bearer_token;
    let metrics_tls_cert = file.global.metrics_tls_cert;
    let metrics_tls_key = file.global.metrics_tls_key;
    let shutdown_timeout = args
        .shutdown_timeout
        .or(file.global.shutdown_timeout)
        .unwrap_or(DEFAULT_SHUTDOWN_TIMEOUT);
    let global_max_connections = file.global.max_connections;
    let global_max_connecting = file.global.max_connecting;
    let global_connect_timeout = file
        .global
        .connect_timeout
        .unwrap_or(DEFAULT_CONNECT_TIMEOUT);
    let global_idle_timeout = file.global.idle_timeout;
    let global_tcp_keepalive = file.global.tcp_keepalive;
    let global_dns_refresh = file.global.dns_refresh;
    let global_socket_buffer_size = file.global.socket_buffer_size;
    let global_buffer_size = file.global.buffer_size.unwrap_or(DEFAULT_BUFFER_SIZE);
    let global_forward_mode = file
        .global
        .forward_mode
        .as_deref()
        .map(ForwardMode::from_str)
        .transpose()?
        .unwrap_or_default();

    let mut relays = Vec::with_capacity(file.relay.len());
    for (index, relay) in file.relay.into_iter().enumerate() {
        let name = relay
            .name
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| format!("relay-{}", index + 1));
        let forward_mode = relay
            .forward_mode
            .as_deref()
            .map(ForwardMode::from_str)
            .transpose()?
            .unwrap_or(global_forward_mode);
        relays.push(RelaySpec {
            name,
            listen: Endpoint::parse(&relay.listen, true, true)?,
            target: Endpoint::parse(&relay.target, false, false)?,
            connect_timeout: relay.connect_timeout.unwrap_or(global_connect_timeout),
            idle_timeout: relay.idle_timeout.or(global_idle_timeout),
            max_connections: relay.max_connections,
            max_connecting: relay.max_connecting,
            tcp_keepalive: relay.tcp_keepalive.or(global_tcp_keepalive),
            dns_refresh: relay.dns_refresh.or(global_dns_refresh),
            socket_buffer_size: relay.socket_buffer_size.or(global_socket_buffer_size),
            buffer_size: relay.buffer_size.unwrap_or(global_buffer_size),
            forward_mode,
        });
    }

    let settings = Settings {
        log_level,
        metrics_listen,
        metrics_bearer_token,
        metrics_tls_cert,
        metrics_tls_key,
        shutdown_timeout,
        global_max_connections,
        global_max_connecting,
        buffer_pool_size: file
            .global
            .buffer_pool_size
            .unwrap_or(DEFAULT_BUFFER_POOL_SIZE),
        connection_log_sample_rate: file
            .global
            .connection_log_sample_rate
            .unwrap_or(DEFAULT_CONNECTION_LOG_SAMPLE_RATE),
        reuse_port: file.global.reuse_port,
        relays,
    };
    settings.validate()?;
    Ok(settings)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_ipv4_hostname_and_listener_shorthand() {
        assert_eq!(
            Endpoint::parse(":13306", true, true).unwrap().address(),
            "0.0.0.0:13306"
        );
        assert_eq!(
            Endpoint::parse("db.internal:3306", false, false)
                .unwrap()
                .host(),
            "db.internal"
        );
    }

    #[test]
    fn parses_bracketed_ipv6() {
        assert_eq!(
            Endpoint::parse("[::1]:5432", false, false)
                .unwrap()
                .address(),
            "[::1]:5432"
        );
    }

    #[test]
    fn rejects_invalid_target_and_port() {
        assert!(Endpoint::parse(":3306", false, false).is_err());
        assert!(Endpoint::parse("127.0.0.1", false, false).is_err());
        assert!(Endpoint::parse("127.0.0.1:0", false, false).is_err());
        assert!(Endpoint::parse("::1:5432", false, false).is_err());
    }

    #[test]
    fn parses_toml_durations_and_rejects_duplicates() {
        let parsed: FileConfig = toml::from_str(
            r#"
                [[relay]]
                name = "one"
                listen = "127.0.0.1:10001"
                target = "127.0.0.1:20001"
                connect_timeout = "2s"

                [[relay]]
                name = "two"
                listen = "127.0.0.1:10001"
                target = "127.0.0.1:20002"
            "#,
        )
        .unwrap();
        let args = ServeArgs {
            listen: None,
            target: None,
            config: None,
            connect_timeout: None,
            idle_timeout: None,
            shutdown_timeout: None,
            max_connections: None,
            max_connecting: None,
            tcp_keepalive: None,
            dns_refresh: None,
            socket_buffer_size: None,
            buffer_size: None,
            buffer_pool_size: None,
            connection_log_sample_rate: None,
            forward_mode: None,
            reuse_port: false,
            metrics_listen: None,
            metrics_token: None,
            metrics_tls_cert: None,
            metrics_tls_key: None,
            log_level: None,
        };
        assert_eq!(
            parsed.relay[0].connect_timeout,
            Some(Duration::from_secs(2))
        );
        assert!(settings_from_file(parsed, &args).is_err());
    }

    #[test]
    fn single_settings_validate() {
        let settings = Settings::single("127.0.0.1:0", "127.0.0.1:1").unwrap();
        assert_eq!(settings.relays[0].name, "default");
    }

    #[test]
    fn rejects_metrics_tls_without_listener() {
        let mut settings = Settings::single("127.0.0.1:0", "127.0.0.1:1").unwrap();
        settings.metrics_tls_cert = Some("cert.pem".into());
        settings.metrics_tls_key = Some("key.pem".into());
        let error = settings.validate().unwrap_err().to_string();
        assert!(error.contains("metrics TLS requires metrics_listen"));
    }
}
