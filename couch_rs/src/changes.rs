use crate::{
    client::Client,
    error::{CouchError, CouchResult},
    types::changes::{ChangeEvent, Event},
};
use futures_core::{Future, Stream};
use futures_util::{FutureExt, StreamExt, TryStreamExt, ready};
use reqwest::{Method, Response, StatusCode};
use std::{
    collections::HashMap,
    io,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tokio::io::AsyncBufReadExt;
use tokio_stream::wrappers::LinesStream;
use tokio_util::io::StreamReader;

/// The max timeout value for longpoll/continous HTTP requests
/// that `CouchDB` supports (see [1]).
///
/// [1]: https://docs.couchdb.org/en/stable/api/database/changes.html
const COUCH_MAX_TIMEOUT: usize = 60000;

/// The stream for the `_changes` endpoint.
///
/// This is returned from [`Database::changes`].
pub struct ChangesStream {
    last_seq: Option<serde_json::Value>,
    client: Client,
    database: String,
    state: ChangesStreamState,
    params: HashMap<String, String>,
    infinite: bool,
    reconnect: bool,
}

enum ChangesStreamState {
    Idle,
    Requesting(Pin<Box<dyn Future<Output = CouchResult<Response>> + Send + Sync + 'static>>),
    Reading(Pin<Box<dyn Stream<Item = io::Result<String>> + Send + Sync + 'static>>),
}

impl ChangesStream {
    /// Create a new changes stream.
    pub fn new(client: Client, database: String, last_seq: Option<serde_json::Value>) -> Self {
        let mut params = HashMap::new();
        params.insert("feed".to_string(), "continuous".to_string());
        params.insert("timeout".to_string(), "0".to_string());
        params.insert("include_docs".to_string(), "true".to_string());
        Self::with_params(client, database, last_seq, params)
    }

    /// Create a new changes stream with params.
    pub fn with_params(
        client: Client,
        database: String,
        last_seq: Option<serde_json::Value>,
        params: HashMap<String, String>,
    ) -> Self {
        Self {
            client,
            database,
            params,
            state: ChangesStreamState::Idle,
            infinite: false,
            reconnect: true,
            last_seq,
        }
    }

    /// Set the starting seq.
    pub fn set_last_seq(&mut self, last_seq: Option<serde_json::Value>) {
        self.last_seq = last_seq;
    }

    /// Set infinite mode.
    ///
    /// If set to true, the changes stream will wait and poll for changes. Otherwise,
    /// the stream will return all changes until now and then close.
    pub fn set_infinite(&mut self, infinite: bool) {
        self.infinite = infinite;
        let timeout = if infinite {
            COUCH_MAX_TIMEOUT.to_string()
        } else {
            0.to_string()
        };
        self.params.insert("timeout".to_string(), timeout);
    }

    /// Set a heartbeat.
    ///
    /// `CouchDB` then sends an empty line every `heartbeat` while there are no changes. A
    /// heartbeat overrides the feed's `timeout`, so the response stays open indefinitely
    /// instead of ending after 60 seconds of quiet, and a client read timeout longer than
    /// the heartbeat tells a quiet feed apart from a dead connection. Empty lines are
    /// skipped, so they never reach the stream's consumer.
    pub fn set_heartbeat(&mut self, heartbeat: Duration) {
        self.params
            .insert("heartbeat".to_string(), heartbeat.as_millis().to_string());
    }

    /// Set whether an infinite stream reopens by itself.
    ///
    /// By default (`true`) an infinite stream sends a new request, at once and without
    /// reporting it, when the response ends or a read times out. With `false` the stream
    /// ends (`None`) when the response ends and returns a read timeout as an error, so the
    /// caller can log it, back off, and open a new stream from [`last_seq`](Self::last_seq).
    pub fn set_reconnect(&mut self, reconnect: bool) {
        self.reconnect = reconnect;
    }

    /// Get the last retrieved seq.
    pub fn last_seq(&self) -> Option<&serde_json::Value> {
        self.last_seq.as_ref()
    }

    /// Whether this stream is running in infinite mode.
    pub fn infinite(&self) -> bool {
        self.infinite
    }
}

async fn get_changes(client: Client, database: String, params: HashMap<String, String>) -> CouchResult<Response> {
    let path = format!("{database}/_changes");
    let res = client.req(Method::GET, &path, Some(&params)).send().await?;
    Ok(res)
}

impl Stream for ChangesStream {
    type Item = CouchResult<ChangeEvent>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            self.state = match self.state {
                ChangesStreamState::Idle => {
                    let mut params = self.params.clone();
                    if let Some(seq) = &self.last_seq {
                        match seq {
                            serde_json::Value::String(s) => {
                                params.insert("since".to_string(), s.into());
                            }

                            _ => {
                                params.insert("since".to_string(), seq.to_string());
                            }
                        }
                    }
                    let fut = get_changes(self.client.clone(), self.database.clone(), params);
                    ChangesStreamState::Requesting(Box::pin(fut))
                }
                ChangesStreamState::Requesting(ref mut fut) => match ready!(fut.poll_unpin(cx)) {
                    Err(err) => return Poll::Ready(Some(Err(err))),
                    Ok(res) => {
                        if res.status().is_success() {
                            let stream = res.bytes_stream().map_err(io::Error::other);
                            let reader = StreamReader::new(stream);
                            let lines = Box::pin(LinesStream::new(reader.lines()));
                            ChangesStreamState::Reading(lines)
                        } else {
                            return Poll::Ready(Some(Err(CouchError::new(
                                res.status().canonical_reason().unwrap().to_string(),
                                res.status(),
                            ))));
                        }
                    }
                },
                ChangesStreamState::Reading(ref mut lines) => {
                    let line = ready!(lines.poll_next_unpin(cx));
                    match line {
                        None if !self.reconnect => return Poll::Ready(None),
                        None => ChangesStreamState::Idle,
                        Some(Err(err)) => {
                            let inner = err.get_ref().and_then(|err| err.downcast_ref::<reqwest::Error>());
                            match inner {
                                Some(reqwest_err) if reqwest_err.is_timeout() && self.infinite && self.reconnect => {
                                    ChangesStreamState::Idle
                                }
                                Some(reqwest_err) => {
                                    return Poll::Ready(Some(Err(CouchError::new(
                                        reqwest_err.to_string(),
                                        reqwest_err.status().unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                                    ))));
                                }
                                _ => {
                                    return Poll::Ready(Some(Err(CouchError::new(
                                        format!("{err}"),
                                        StatusCode::from_u16(500).unwrap(),
                                    ))));
                                }
                            }
                        }
                        Some(Ok(line)) if line.is_empty() => continue,
                        Some(Ok(line)) => match serde_json::from_str::<Event>(&line) {
                            Ok(Event::Change(event)) => {
                                self.last_seq = Some(event.seq.clone());
                                return Poll::Ready(Some(Ok(event)));
                            }
                            Ok(Event::Finished(event)) => {
                                self.last_seq = Some(event.last_seq.clone());
                                if !self.infinite || !self.reconnect {
                                    return Poll::Ready(None);
                                }
                                ChangesStreamState::Idle
                            }
                            Err(e) => {
                                return Poll::Ready(Some(Err(e.into())));
                            }
                        },
                    }
                }
            }
        }
    }
}

#[cfg(feature = "integration-tests")]
#[cfg(test)]
mod tests {
    use crate::client::Client;
    use futures_util::StreamExt;
    use serde_json::{Value, json};
    use tokio::join;

    #[tokio::test]
    async fn should_get_changes() {
        let client = Client::new_local_test().unwrap();
        let db = client.db("should_get_changes").await.unwrap();
        let mut changes = db.changes(None);
        changes.set_infinite(true);
        let t = tokio::spawn({
            let db = db.clone();
            async move {
                let mut docs: Vec<Value> = (0..10)
                    .map(|idx| {
                        json!({
                            "_id": format!("test_{}", idx),
                            "count": idx,
                        })
                    })
                    .collect();

                db.bulk_docs(&mut docs).await.expect("should insert 10 documents");
            }
        });

        let mut collected_changes = vec![];
        while let Some(change) = changes.next().await {
            collected_changes.push(change);
            if collected_changes.len() == 10 {
                break;
            }
        }

        assert_eq!(collected_changes.len(), 10, "should collect 10 changes");
        t.await.unwrap();
    }

    #[tokio::test]
    async fn can_stream_changes_async() {
        let client = Client::new_local_test().unwrap();
        let db = client.db("should_get_changes").await.unwrap();
        let mut changes = db.changes(None);
        changes.set_infinite(true);

        let (tx, mut rx) = tokio::sync::mpsc::channel(10);

        let db_update_task = tokio::spawn({
            let db = db.clone();
            async move {
                let mut docs: Vec<Value> = (0..10)
                    .map(|idx| {
                        json!({
                            "_id": format!("test_async_{}", idx),
                            "count": idx,
                        })
                    })
                    .collect();

                db.bulk_docs(&mut docs).await.expect("should insert 10 documents");
            }
        });

        let monitor_task = tokio::spawn(async move {
            let mut iterations = 0;
            while let Some(change) = changes.next().await {
                tx.send(change).await.expect("should send change");

                // stop at 10
                iterations += 1;
                if iterations == 10 {
                    break;
                }
            }
        });

        let mut collected_changes = vec![];
        while let Some(change) = rx.recv().await {
            collected_changes.push(change);
            if collected_changes.len() == 10 {
                break;
            }
        }

        assert_eq!(collected_changes.len(), 10, "should collect 10 changes");
        let _ = join!(db_update_task, monitor_task);
    }
}

#[cfg(test)]
mod heartbeat_tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    /// A server that answers every `_changes` request with one change and then ends
    /// the response cleanly, counting the requests it gets.
    async fn closing_server() -> (String, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&requests);
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let counter = Arc::clone(&counter);
                tokio::spawn(async move {
                    let mut head = Vec::new();
                    let mut buf = [0u8; 1024];
                    while !head.windows(4).any(|w| w == b"\r\n\r\n") {
                        let Ok(n) = socket.read(&mut buf).await else { return };
                        if n == 0 {
                            return;
                        }
                        head.extend_from_slice(&buf[..n]);
                    }
                    let n = counter.fetch_add(1, Ordering::SeqCst) + 1;
                    let line = format!("{{\"seq\":\"{n}-a\",\"id\":\"doc{n}\",\"changes\":[{{\"rev\":\"1-a\"}}]}}\n");
                    let body = format!("{:x}\r\n{line}\r\n0\r\n\r\n", line.len());
                    let response = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ntransfer-encoding: chunked\r\nconnection: close\r\n\r\n{body}"
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                });
            }
        });
        (url, requests)
    }

    #[tokio::test]
    async fn an_infinite_stream_reopens_by_itself_by_default() {
        let (url, requests) = closing_server().await;
        let client = Client::new_no_auth(&url).unwrap();
        let mut changes = ChangesStream::new(client, "db".to_string(), None);
        changes.set_infinite(true);

        assert_eq!(changes.next().await.unwrap().unwrap().id, "doc1");
        assert_eq!(changes.next().await.unwrap().unwrap().id, "doc2");
        assert_eq!(requests.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn without_reconnect_an_infinite_stream_ends_with_the_response() {
        let (url, requests) = closing_server().await;
        let client = Client::new_no_auth(&url).unwrap();
        let mut changes = ChangesStream::new(client, "db".to_string(), None);
        changes.set_infinite(true);
        changes.set_reconnect(false);

        assert_eq!(changes.next().await.unwrap().unwrap().id, "doc1");
        assert!(changes.next().await.is_none(), "the stream should end, not reopen");
        assert_eq!(requests.load(Ordering::SeqCst), 1);
        assert_eq!(changes.last_seq(), Some(&serde_json::Value::String("1-a".to_string())));
    }

    #[test]
    fn set_heartbeat_sends_milliseconds() {
        let client = Client::new_no_auth("http://localhost:5984").unwrap();
        let mut changes = ChangesStream::new(client, "db".to_string(), None);

        changes.set_heartbeat(Duration::from_secs(30));

        assert_eq!(changes.params.get("heartbeat").map(String::as_str), Some("30000"));
    }
}
