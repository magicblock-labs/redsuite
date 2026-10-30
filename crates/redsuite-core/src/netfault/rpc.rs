use super::{Operation, Stage, Transport};
use crate::api::decode_transaction;
use json::{JsonContainerTrait, JsonValueTrait, Value};

pub(super) fn id(value: &Value) -> Option<String> {
    value.get("id").map(ToString::to_string)
}

pub(super) fn subscription(value: &Value) -> Option<u64> {
    value["params"]["subscription"].as_u64()
}

// Metadata is best-effort; relays forward the original payload even when it is not JSON-RPC.
pub(super) fn request(transport: Transport, value: &Value) -> Operation {
    let method = value["method"].as_str().unwrap_or("?");
    let first = &value["params"][0];
    let mut operation = Operation {
        transport,
        stage: Stage::Request,
        method: method.to_owned(),
        signatures: Vec::new(),
        accounts: Vec::new(),
    };
    match method {
        "getAccountInfo" | "accountSubscribe" | "programSubscribe" => {
            operation.accounts.extend(first.as_str().map(str::to_owned));
        }
        "signatureSubscribe" | "getTransaction" => {
            operation.signatures.extend(first.as_str().map(str::to_owned));
        }
        "getMultipleAccounts" | "getSignatureStatuses" => {
            let targets = if method == "getMultipleAccounts" {
                &mut operation.accounts
            } else {
                &mut operation.signatures
            };
            if let Some(values) = first.as_array() {
                targets.extend(values.iter().filter_map(|value| value.as_str()).map(str::to_owned));
            }
        }
        "sendTransaction" => {
            if let Some(tx) = first.as_str().and_then(|text| decode_transaction(text).ok()) {
                operation.signatures.extend(tx.signatures.iter().map(ToString::to_string));
                operation
                    .accounts
                    .extend(tx.message.static_account_keys().iter().map(ToString::to_string));
            }
        }
        _ => {}
    }
    operation
}

pub(super) fn notification(value: &Value, base: Option<Operation>) -> Operation {
    let mut operation = base.unwrap_or_else(|| request(Transport::Ws, &Value::new()));
    operation.stage = Stage::Notification;
    operation.method = value["method"].as_str().unwrap_or("?").to_owned();
    if operation.method == "programNotification" {
        operation
            .accounts
            .extend(value["params"]["result"]["value"]["pubkey"].as_str().map(str::to_owned));
    }
    operation
}

pub(super) fn rejection(value: &Value, message: &str) -> String {
    json::json!({
        "jsonrpc": "2.0",
        "error": { "code": -32000, "message": message },
        "id": value["id"],
    })
    .to_string()
}
