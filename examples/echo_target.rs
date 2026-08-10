use std::{net::SocketAddr, time::Duration};

use anyhow::Result;
use clap::Parser;
use socket2::{Domain, Protocol, Socket, Type};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;

const LISTEN_BACKLOG: i32 = 65_535;
const BUFFER_SIZE: usize = 16 * 1024;

#[derive(Debug, Parser)]
#[command(about = "High-concurrency generic TCP echo target for relay load tests")]
struct Args {
    /// TCP listener. Repeat for multiple target shards.
    #[arg(long = "listen", value_name = "HOST:PORT", required = true)]
    listen: Vec<SocketAddr>,
    /// Delay each echo response to exercise relay backpressure.
    #[arg(long, default_value_t = 0)]
    response_delay_ms: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let response_delay = Duration::from_millis(args.response_delay_ms);
    let mut listeners = Vec::with_capacity(args.listen.len());
    for address in args.listen {
        listeners.push(bind_listener(address)?);
    }

    println!(
        "echo_target_ready listeners={} response_delay_ms={}",
        listeners.len(),
        args.response_delay_ms
    );
    let shutdown = CancellationToken::new();
    let signal_shutdown = shutdown.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        signal_shutdown.cancel();
    });

    let mut acceptors = JoinSet::new();
    for listener in listeners {
        let listener_response_delay = response_delay;
        let listener_shutdown = shutdown.clone();
        acceptors.spawn(async move {
            let mut clients = JoinSet::new();
            loop {
                tokio::select! {
                    _ = listener_shutdown.cancelled() => break,
                    accepted = listener.accept() => {
                        let Ok((stream, _)) = accepted else { break };
                        clients.spawn(echo(stream, listener_response_delay));
                    }
                }
            }
            clients.abort_all();
            while clients.join_next().await.is_some() {}
        });
    }

    while acceptors.join_next().await.is_some() {}
    Ok(())
}

fn bind_listener(address: SocketAddr) -> Result<TcpListener> {
    let socket = Socket::new(
        Domain::for_address(address),
        Type::STREAM,
        Some(Protocol::TCP),
    )?;
    socket.set_reuse_address(true)?;
    socket.bind(&address.into())?;
    socket.listen(LISTEN_BACKLOG)?;
    socket.set_nonblocking(true)?;
    Ok(TcpListener::from_std(socket.into())?)
}

async fn echo(mut stream: TcpStream, response_delay: Duration) {
    let mut buffer = [0_u8; BUFFER_SIZE];
    loop {
        let Ok(read) = stream.read(&mut buffer).await else {
            return;
        };
        if read == 0 {
            return;
        }
        if !response_delay.is_zero() {
            tokio::time::sleep(response_delay).await;
        }
        if stream.write_all(&buffer[..read]).await.is_err() {
            return;
        }
    }
}
