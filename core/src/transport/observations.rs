use std::{
    ops::ControlFlow::{Break, Continue},
    rc::Rc,
};

use pubkey::Pubkey;
use signature::Signature;
use tokio::sync::Notify;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use url::Url;

use super::{
    socket::CloseReason,
    subscription::{self, Connection, SubscriptionState, Topic},
};
use crate::Result;

#[derive(Clone, Copy, PartialEq)]
enum Key {
    Account(usize),
    Signature(u64),
}

#[derive(Clone, Copy)]
pub enum ObservationClose {
    GoingAway,
    Transport,
    Other,
}

#[derive(Default)]
struct Observer {
    close_reason: Option<ObservationClose>,
    closed: Rc<Notify>,
}

impl SubscriptionState for Observer {
    type Key = Key;

    fn on_close(&mut self, reason: &CloseReason) {
        self.close_reason = Some(match reason {
            CloseReason::ServerClose(Some(frame)) if frame.code == CloseCode::Away => {
                ObservationClose::GoingAway
            }
            CloseReason::Transport(_) => ObservationClose::Transport,
            _ => ObservationClose::Other,
        });
        self.closed.notify_waiters();
    }
}

pub enum Notification {
    Account { index: usize, data: Vec<u8> },
    Signature { id: u64, succeeded: bool },
}

pub struct ObservationSocket {
    connection: Connection<Observer>,
}

impl ObservationSocket {
    pub async fn connect(
        url: &Url,
        mut observe: impl FnMut(Notification) + 'static,
    ) -> Result<Self> {
        let connection =
            Connection::connect(url, Observer::default(), move |_, key, payload| match key {
                Key::Account(index) => {
                    if let Some(data) = subscription::account_data(payload) {
                        observe(Notification::Account { index, data });
                    }
                    Continue(())
                }
                Key::Signature(id) => {
                    let Some(result) = subscription::processed_signature(payload) else {
                        return Continue(());
                    };
                    observe(Notification::Signature {
                        id,
                        succeeded: result.err.is_none(),
                    });
                    Break(())
                }
            })
            .await?;
        Ok(Self { connection })
    }

    pub async fn account(&self, index: usize, address: &Pubkey) -> Result<()> {
        self.connection
            .subscribe_and_wait(
                Topic::Account,
                &subscription::account_params(address),
                Key::Account(index),
            )
            .await
    }

    pub async fn signature(&self, id: u64, signature: &Signature) -> Result<()> {
        self.connection
            .subscribe_and_wait(
                Topic::Signature,
                &subscription::signature_params(signature),
                Key::Signature(id),
            )
            .await
    }

    pub async fn unsubscribe_signature(&self, id: u64) -> Result<()> {
        self.connection.unsubscribe(Key::Signature(id), Topic::Signature).await
    }

    pub fn is_open(&self) -> bool {
        self.connection.check_error().is_ok()
    }

    pub async fn closed(&self) -> ObservationClose {
        let closed = self.connection.state().closed.clone();
        let notification = closed.notified();
        if let Some(reason) = self.connection.state().close_reason {
            return reason;
        }
        notification.await;
        self.connection.state().close_reason.expect("close notification has a reason")
    }
}
