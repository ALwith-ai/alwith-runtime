//! Bounded NDJSON framing shared by ACP and Runtime transports.
use futures_util::StreamExt;
use tokio::io::AsyncRead;
use tokio_util::codec::{FramedRead, LinesCodec};

/// Matches ACP SDK 1.5.1's default incoming message byte limit.
pub const MAX_MESSAGE_BYTES: usize = 32 * 1024 * 1024;
pub const MAX_BUFFERED_BYTES: usize = 64 * 1024 * 1024;

pub struct ProtocolLines<R> {
    inner: FramedRead<R, LinesCodec>,
}

pub fn protocol_lines<R: AsyncRead>(reader: R) -> ProtocolLines<R> {
    ProtocolLines { inner: FramedRead::new(reader, LinesCodec::new_with_max_length(MAX_MESSAGE_BYTES)) }
}

impl<R: AsyncRead + Unpin> ProtocolLines<R> {
    /// Cancellation-safe: a partially received line remains in the codec.
    pub async fn next_line(&mut self) -> std::io::Result<Option<String>> {
        self.inner.next().await.transpose().map_err(std::io::Error::other)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn oversized_unterminated_input_fails_without_waiting_for_eof() {
        let (mut writer, reader) = tokio::io::duplex(8192);
        let writing = tokio::spawn(async move {
            let _ = writer.write_all(&vec![b'x'; MAX_MESSAGE_BYTES + 1]).await;
            // Keep the stream open: rejection must depend on size, not EOF.
            std::future::pending::<()>().await;
        });
        let mut lines = protocol_lines(reader);
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), lines.next_line()).await.unwrap();
        assert!(result.is_err());
        writing.abort();
    }

    #[tokio::test]
    async fn cancelled_read_preserves_fragmented_utf8() {
        let (mut writer, reader) = tokio::io::duplex(64);
        let mut lines = protocol_lines(reader);
        writer.write_all(&[0xe4, 0xb8]).await.unwrap();
        assert!(tokio::time::timeout(std::time::Duration::from_millis(1), lines.next_line()).await.is_err());
        writer.write_all(&[0xad, b'\n']).await.unwrap();
        assert_eq!(lines.next_line().await.unwrap().as_deref(), Some("中"));
    }
}
