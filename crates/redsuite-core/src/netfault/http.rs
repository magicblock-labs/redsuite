use super::{gate, merged, operation, parse, rejection, Decision, Shared, Stage, Transport};
use crate::transport::http::{self, TransportError};
use reqwest::{Client, StatusCode};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task,
};
use url::Url;

const MAX_HEAD: usize = 64 * 1024;
const MAX_BODY: usize = 16 * 1024 * 1024;

pub(super) async fn accept(listener: TcpListener, upstream: Url, client: Client, shared: Shared) {
    loop {
        let Ok((socket, _)) = listener.accept().await else {
            return;
        };
        let handle = task::spawn_local(serve_http(
            socket,
            upstream.clone(),
            client.clone(),
            shared.clone(),
        ));
        shared.borrow_mut().track(handle);
    }
}

async fn read_request(socket: &mut TcpStream, buffer: &mut Vec<u8>) -> Option<(String, String)> {
    loop {
        if let Some(split) = buffer.windows(4).position(|w| w == b"\r\n\r\n") {
            let head = String::from_utf8_lossy(&buffer[..split]).into_owned();
            let content_length = head
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.trim()
                        .eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().ok())
                        .flatten()
                })
                .unwrap_or(0);
            if content_length > MAX_BODY {
                return None;
            }
            let body_start = split + 4;
            while buffer.len() < body_start + content_length {
                let mut chunk = [0u8; 16 * 1024];
                let read = socket.read(&mut chunk).await.ok()?;
                if read == 0 {
                    return None;
                }
                buffer.extend_from_slice(&chunk[..read]);
            }
            let body = String::from_utf8_lossy(&buffer[body_start..body_start + content_length])
                .into_owned();
            buffer.drain(..body_start + content_length);
            return Some((head, body));
        }
        if buffer.len() > MAX_HEAD {
            return None;
        }
        let mut chunk = [0u8; 16 * 1024];
        let read = socket.read(&mut chunk).await.ok()?;
        if read == 0 {
            return None;
        }
        buffer.extend_from_slice(&chunk[..read]);
    }
}

async fn write_response(socket: &mut TcpStream, status: StatusCode, body: &str) -> bool {
    let reason = if status == StatusCode::OK { "OK" } else { "Error" };
    let code = status.as_str();
    let head = format!(
        "HTTP/1.1 {code} {reason}\r\ncontent-type: application/json\r\n\
         content-length: {}\r\n\r\n",
        body.len()
    );
    socket.write_all(head.as_bytes()).await.is_ok()
        && socket.write_all(body.as_bytes()).await.is_ok()
        && socket.flush().await.is_ok()
}

async fn serve_http(mut socket: TcpStream, upstream: Url, client: Client, shared: Shared) {
    let mut buffer = Vec::new();
    loop {
        let Some((head, body)) = read_request(&mut socket, &mut buffer).await else {
            return;
        };
        let keep_alive = !head.lines().any(|line| {
            line.split_once(':').is_some_and(|(name, value)| {
                name.trim().eq_ignore_ascii_case("connection")
                    && value.trim().eq_ignore_ascii_case("close")
            })
        });
        let parsed = parse(&body);
        let (id, request) = match &parsed {
            Some(parsed) => (
                parsed.id.clone(),
                operation(
                    Transport::Http,
                    Stage::Request,
                    parsed.method.as_deref().unwrap_or("?"),
                    parsed.strings.clone(),
                ),
            ),
            None => (
                None,
                operation(Transport::Http, Stage::Request, "?", Vec::new()),
            ),
        };
        let (decision, _request_hold) = gate(&shared, request.clone()).await;
        match decision {
            Decision::Discard => return,
            Decision::Reject(message) => {
                if !write_response(&mut socket, StatusCode::OK, &rejection(&id, &message)).await {
                    return;
                }
                if !keep_alive {
                    return;
                }
                continue;
            }
            Decision::Release => {}
        }
        let (status, text) = match http::post_json(&client, &upstream, body).await {
            Ok(text) => (StatusCode::OK, text),
            Err(TransportError::Status { status, body }) => (status, body),
            Err(error) => (StatusCode::BAD_GATEWAY, error.to_string()),
        };
        let extra = parse(&text).map(|parsed| parsed.strings).unwrap_or_default();
        let response = merged(request, Stage::Response, extra);
        let (decision, _response_hold) = gate(&shared, response).await;
        match decision {
            Decision::Discard => return,
            Decision::Reject(message) => {
                if !write_response(&mut socket, StatusCode::OK, &rejection(&id, &message)).await {
                    return;
                }
            }
            Decision::Release => {
                if !write_response(&mut socket, status, &text).await {
                    return;
                }
            }
        }
        if !keep_alive {
            return;
        }
    }
}
