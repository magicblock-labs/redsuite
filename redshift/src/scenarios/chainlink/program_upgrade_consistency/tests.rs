use super::{release_after_cold_fetch, Progress};
use anyhow::{ensure, Context};
use pubkey::Pubkey;
use redsuite_core::{
    netfault::{Faults, Proxy, RuleHandle, Selector},
    topology::BaseEndpoints,
    transport::http,
    Result,
};
use std::{rc::Rc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::oneshot,
    task::{self, JoinHandle, LocalSet},
    time::timeout,
};

const DEADLINE: Duration = Duration::from_secs(5);
const UPGRADED: &str = r#"{"jsonrpc":"2.0","id":1,"result":{"value":"upgraded bytes"}}"#;

// The base endpoint returns upgraded bytes but emits no WebSocket notification.
// This reproduces a missed/nonreplayed upgrade notification without a validator.
async fn base_server(requests: usize) -> Result<(BaseEndpoints, JoinHandle<Result<()>>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let task = task::spawn_local(async move {
        for _ in 0..requests {
            let (mut socket, _) = listener.accept().await?;
            let mut request = Vec::new();
            loop {
                let mut chunk = [0; 1024];
                let read = socket.read(&mut chunk).await?;
                ensure!(read != 0, "request closed before its body arrived");
                request.extend_from_slice(&chunk[..read]);
                if let Some(split) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                    let head = std::str::from_utf8(&request[..split])?;
                    let length = head
                        .lines()
                        .filter_map(|line| line.split_once(':'))
                        .find(|(key, _)| key.eq_ignore_ascii_case("content-length"))
                        .context("missing request length")?
                        .1
                        .trim()
                        .parse::<usize>()?;
                    if request.len() >= split + 4 + length {
                        break;
                    }
                }
            }
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{UPGRADED}",
                UPGRADED.len(),
            );
            socket.write_all(response.as_bytes()).await?;
            socket.shutdown().await?;
        }
        Ok(())
    });
    Ok((
        BaseEndpoints {
            rpc_url: format!("http://{address}").parse()?,
            ws_url: format!("ws://{address}").parse()?,
            grpc_url: format!("http://{address}").parse()?,
        },
        task,
    ))
}

fn fetch_selector(program: &Pubkey) -> Selector {
    Selector::methods(&["getAccountInfo", "getMultipleAccounts"])
        .http()
        .response()
        .account(program)
}

fn upgrade_holds(faults: &Faults, keys: [Pubkey; 2]) -> [[RuleHandle; 2]; 2] {
    keys.map(|key| {
        [
            faults.stall(
                Selector::methods(&["accountNotification", "programNotification"])
                    .ws()
                    .notification()
                    .account(&key),
            ),
            faults.stall(fetch_selector(&key)),
        ]
    })
}

#[tokio::test(flavor = "current_thread")]
async fn cold_fetch_releases_upgrade_holds_without_websocket_delivery() -> Result<()> {
    LocalSet::new()
        .run_until(async {
            let (base, server) = base_server(3).await?;
            let hot_proxy = Proxy::spawn(base.clone()).await?;
            let cold_proxy = Proxy::spawn(base).await?;
            let hot_faults = hot_proxy.scope()?;
            let cold_faults = cold_proxy.scope()?;
            let program = Pubkey::new_unique();
            let programdata = Pubkey::new_unique();
            let hot_hit = hot_faults.observe(fetch_selector(&program));
            let ws_hit = hot_faults.observe(Selector::default().ws().notification());
            let holds = upgrade_holds(&hot_faults, [program, programdata]);
            let cold_fetch = cold_faults.stall(fetch_selector(&program));
            let body = format!(
                r#"{{"jsonrpc":"2.0","id":1,"method":"getAccountInfo","params":["{program}"]}}"#
            );

            // Let only a hot request hit first. A hot sweep must not satisfy the
            // separate cold-fetch rendezvous and prematurely release the holds.
            let (start_cold, cold_started) = oneshot::channel();
            let cold_url = cold_proxy.endpoints().rpc_url;
            let cold_body = body.clone();
            let progress = Rc::new(Progress::default());
            progress.phase.set(1);
            let release_progress = progress.clone();
            let mut release = task::spawn_local(async move {
                release_after_cold_fetch(holds, cold_fetch, &release_progress, async move {
                    cold_started.await?;
                    Ok(http::post_json(&http::client(), &cold_url, cold_body).await?)
                })
                .await
            });
            let hot_url = hot_proxy.endpoints().rpc_url;
            let mut hot_request =
                task::spawn_local(
                    async move { http::post_json(&http::client(), &hot_url, body).await },
                );
            hot_hit.wait_hit(DEADLINE).await?;
            ensure!(timeout(Duration::from_millis(25), &mut hot_request).await.is_err());
            ensure!(!release.is_finished());
            ensure!(progress.phase.get() == 1);
            ensure!(ws_hit.hits() == 0);

            start_cold.send(()).expect("cold request still waits for its start signal");
            ensure!(timeout(DEADLINE, &mut release).await??? == UPGRADED);
            ensure!(timeout(DEADLINE, &mut hot_request).await??? == UPGRADED);
            ensure!(progress.phase.get() == 2);
            ensure!(
                ws_hit.hits() == 0,
                "this case must not depend on a WS notification"
            );

            // Both HTTP key rules must have been released, including the
            // ProgramData rule that was never needed to trigger cold loading.
            let body = format!(
                r#"{{"jsonrpc":"2.0","id":1,"method":"getAccountInfo","params":["{programdata}"]}}"#
            );
            let response = timeout(
                DEADLINE,
                http::post_json(&http::client(), &hot_proxy.endpoints().rpc_url, body),
            )
            .await??;
            ensure!(response == UPGRADED);
            timeout(DEADLINE, server).await???;
            hot_faults.reset().await?;
            cold_faults.reset().await?;
            Ok(())
        })
        .await
}
