//! Probe a WorkerIngest endpoint exactly the way a lane does: connect, open
//! the stream, send only the hello frame, then close. Sends no sub-batches,
//! so nothing enters the worker's pipeline.
//!
//! Usage: cargo run -p ingestion-consumer --example grpc_probe -- http://HOST:PORT

use std::time::Duration;

use ingestion_worker_proto::ingestion::worker::v1::worker_ingest_client::WorkerIngestClient;
use ingestion_worker_proto::ingestion::worker::v1::{
    ingest_stream_request, IngestStreamRequest, StreamHello,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;

#[tokio::main]
async fn main() {
    let addr = std::env::args()
        .nth(1)
        .expect("usage: grpc_probe http://host:port");

    let endpoint = tonic::transport::Endpoint::from_shared(addr.clone())
        .expect("valid address")
        .connect_timeout(Duration::from_secs(5));
    let channel = match endpoint.connect().await {
        Ok(channel) => {
            eprintln!("connect: OK");
            channel
        }
        Err(err) => {
            eprintln!("connect: FAILED — {err:?}");
            return;
        }
    };

    let mut client = WorkerIngestClient::new(channel)
        .send_compressed(tonic::codec::CompressionEncoding::Gzip)
        .accept_compressed(tonic::codec::CompressionEncoding::Gzip);

    let (tx, rx) = mpsc::unbounded_channel();
    tx.send(IngestStreamRequest {
        msg: Some(ingest_stream_request::Msg::Hello(StreamHello {
            consumer_id: "grpc-probe".to_string(),
            stream_epoch: 1,
        })),
    })
    .expect("send hello");

    let open = tokio::time::timeout(
        Duration::from_secs(10),
        client.ingest_stream(UnboundedReceiverStream::new(rx)),
    )
    .await;
    let Ok(open) = open else {
        eprintln!("stream open: HUNG — no response headers within 10s (deadlock)");
        return;
    };
    match open {
        Ok(response) => {
            eprintln!("stream open: OK");
            drop(tx);
            let mut acks = response.into_inner();
            match acks.message().await {
                Ok(None) => eprintln!("stream closed cleanly by server"),
                Ok(Some(ack)) => eprintln!("unexpected ack: {ack:?}"),
                Err(status) => eprintln!("stream ended with status: {status:?}"),
            }
        }
        Err(status) => {
            eprintln!("stream open: FAILED — {status:?}");
        }
    }
}
