#![allow(dead_code)]

use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

static SERVER_STARTUP: Mutex<()> = Mutex::new(());

pub struct Server {
    child: Option<Child>,
    pub port: u16,
    http_port: u16,
    data_dir: PathBuf,
    ca_file: Option<PathBuf>,
}

impl Server {
    pub fn start() -> Self {
        Self::start_with_env(&[])
    }

    pub fn start_with_env(extra_env: &[(&str, &str)]) -> Self {
        Self::start_config(extra_env, false)
    }

    pub fn start_tls() -> Self {
        Self::start_config(&[], true)
    }

    pub fn ca_file(&self) -> Option<PathBuf> {
        self.ca_file.clone()
    }

    fn start_config(extra_env: &[(&str, &str)], tls: bool) -> Self {
        let _startup_guard = SERVER_STARTUP
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let (tcp_reservation, http_reservation, port, http_port) = reserve_port_pair();
        let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let data_dir = std::env::temp_dir().join(format!("bunqueue-rust-test-{suffix}"));
        std::fs::create_dir_all(&data_dir).expect("create test data directory");
        let mut command = Command::new("bun");
        command
            .arg("src/main.ts")
            .current_dir(repo)
            .env("TCP_PORT", port.to_string())
            .env("HTTP_PORT", http_port.to_string())
            .env("BUNQUEUE_DATA_PATH", data_dir.join("bunq.db"));
        for (key, value) in extra_env {
            command.env(key, value);
        }
        let ca_file = if tls {
            let ca_certificate = data_dir.join("ca-cert.pem");
            let ca_key = data_dir.join("ca-key.pem");
            let certificate = data_dir.join("cert.pem");
            let key = data_dir.join("key.pem");
            let request = data_dir.join("server.csr");
            let extensions = data_dir.join("server-ext.cnf");
            std::fs::write(
                &extensions,
                "[v3_req]\nbasicConstraints=critical,CA:FALSE\n\
                 keyUsage=critical,digitalSignature,keyEncipherment\n\
                 extendedKeyUsage=serverAuth\n\
                 subjectAltName=DNS:localhost,IP:127.0.0.1\n",
            )
            .expect("write certificate extensions");
            run_openssl(&[
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-keyout",
                ca_key.to_str().expect("UTF-8 CA key path"),
                "-out",
                ca_certificate.to_str().expect("UTF-8 CA certificate path"),
                "-days",
                "2",
                "-nodes",
                "-subj",
                "/CN=bunqueue Rust Test CA",
                "-addext",
                "basicConstraints=critical,CA:TRUE",
                "-addext",
                "keyUsage=critical,keyCertSign,cRLSign",
            ]);
            run_openssl(&[
                "req",
                "-newkey",
                "rsa:2048",
                "-keyout",
                key.to_str().expect("UTF-8 key path"),
                "-out",
                request.to_str().expect("UTF-8 request path"),
                "-nodes",
                "-subj",
                "/CN=localhost",
            ]);
            run_openssl(&[
                "x509",
                "-req",
                "-in",
                request.to_str().expect("UTF-8 request path"),
                "-CA",
                ca_certificate.to_str().expect("UTF-8 CA certificate path"),
                "-CAkey",
                ca_key.to_str().expect("UTF-8 CA key path"),
                "-CAcreateserial",
                "-out",
                certificate.to_str().expect("UTF-8 certificate path"),
                "-days",
                "2",
                "-extfile",
                extensions.to_str().expect("UTF-8 extensions path"),
                "-extensions",
                "v3_req",
            ]);
            command
                .env("TLS_CERT_FILE", &certificate)
                .env("TLS_KEY_FILE", &key);
            Some(ca_certificate)
        } else {
            None
        };
        drop((tcp_reservation, http_reservation));
        let child = command
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn bunqueue server");
        let server = Self {
            child: Some(child),
            port,
            http_port,
            data_dir,
            ca_file,
        };
        let deadline = Instant::now() + Duration::from_secs(15);
        while Instant::now() < deadline {
            if TcpStream::connect(("127.0.0.1", port)).is_ok() {
                return server;
            }
            thread::sleep(Duration::from_millis(100));
        }
        panic!("bunqueue server did not start within 15 seconds");
    }

    pub fn crash(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    pub fn restart(&mut self) {
        assert!(
            self.ca_file.is_none(),
            "restart helper currently supports plain test brokers"
        );
        self.crash();
        let _startup_guard = SERVER_STARTUP
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let child = Command::new("bun")
            .arg("src/main.ts")
            .current_dir(repo)
            .env("TCP_PORT", self.port.to_string())
            .env("HTTP_PORT", self.http_port.to_string())
            .env("BUNQUEUE_DATA_PATH", self.data_dir.join("bunq.db"))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("restart bunqueue server");
        self.child = Some(child);
        let deadline = Instant::now() + Duration::from_secs(15);
        while Instant::now() < deadline {
            if TcpStream::connect(("127.0.0.1", self.port)).is_ok() {
                return;
            }
            thread::sleep(Duration::from_millis(100));
        }
        panic!("bunqueue server did not restart within 15 seconds");
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.crash();
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

fn run_openssl(arguments: &[&str]) {
    let output = Command::new("openssl")
        .args(arguments)
        .output()
        .expect("run openssl");
    assert!(
        output.status.success(),
        "openssl failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn reserve_port_pair() -> (TcpListener, TcpListener, u16, u16) {
    let tcp_listener = TcpListener::bind(("127.0.0.1", 0)).expect("reserve TCP port");
    let http_listener = TcpListener::bind(("127.0.0.1", 0)).expect("reserve HTTP port");
    let tcp_port = tcp_listener
        .local_addr()
        .expect("read reserved TCP port")
        .port();
    let http_port = http_listener
        .local_addr()
        .expect("read reserved HTTP port")
        .port();
    (tcp_listener, http_listener, tcp_port, http_port)
}
