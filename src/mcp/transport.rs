use std::future::Future;
use std::io;

use axum::extract::ws::Message;
use rmcp::model::ServerJsonRpcMessage;
use rmcp::service::{RoleClient, RxJsonRpcMessage, TxJsonRpcMessage};
use rmcp::transport::Transport;
use serde_json::json;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::RPC_TIMEOUT;

pub(super) struct EnvelopeTransport {
    pub session_id: String,
    pub ws_tx: mpsc::Sender<Message>,
    pub rx: mpsc::Receiver<ServerJsonRpcMessage>,
    pub closed: CancellationToken,
}

impl Transport<RoleClient> for EnvelopeTransport {
    type Error = io::Error;

    fn send(
        &mut self,
        item: TxJsonRpcMessage<RoleClient>,
    ) -> impl Future<Output = io::Result<()>> + Send + 'static {
        let tx = self.ws_tx.clone();
        let closed = self.closed.clone();
        let value = json!({"type":"mcp", "session_id":self.session_id, "payload":item});
        async move {
            tokio::select! {
                _ = closed.cancelled() => Err(io::Error::new(io::ErrorKind::BrokenPipe, "device disconnected")),
                result = tokio::time::timeout(RPC_TIMEOUT, tx.send(Message::Text(value.to_string().into()))) => {
                    result.map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "MCP writer queue timeout"))?
                        .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "WS writer closed"))
                }
            }
        }
    }

    async fn receive(&mut self) -> Option<RxJsonRpcMessage<RoleClient>> {
        tokio::select! {
            _ = self.closed.cancelled() => None,
            item = self.rx.recv() => item,
        }
    }

    async fn close(&mut self) -> io::Result<()> {
        self.rx.close();
        Ok(())
    }
}
