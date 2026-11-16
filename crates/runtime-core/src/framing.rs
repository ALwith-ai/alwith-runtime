//! Bounded NDJSON framing shared by ACP and Runtime transports.
use bytes::{Buf, BytesMut};
use futures_util::StreamExt;
use serde_json::Value;
use tokio::io::AsyncRead;
use tokio_util::codec::{Decoder, FramedRead};

/// Matches ACP SDK 1.5.1's default incoming message byte limit.
pub const MAX_MESSAGE_BYTES: usize = 32 * 1024 * 1024;
pub const MAX_BUFFERED_BYTES: usize = 64 * 1024 * 1024;
/// How much of a discarded message is kept to find out whom it was addressed to.
const OVERSIZED_HEAD_BYTES: usize = 64 * 1024;

/// One received message, or the start of one that exceeded [`MAX_MESSAGE_BYTES`]. The rest of an
/// oversized message is discarded up to its newline, and the stream continues with the next one.
#[derive(Debug, PartialEq, Eq)]
pub enum ProtocolFrame {
    Line(String),
    Oversized { head: String },
}

pub struct ProtocolLines<R> {
    inner: FramedRead<R, BoundedLines>,
}

pub fn protocol_lines<R: AsyncRead>(reader: R) -> ProtocolLines<R> {
    ProtocolLines { inner: FramedRead::new(reader, BoundedLines::default()) }
}

impl<R: AsyncRead + Unpin> ProtocolLines<R> {
    /// Cancellation-safe: a partially received line remains in the codec.
    pub async fn next_frame(&mut self) -> std::io::Result<Option<ProtocolFrame>> {
        self.inner.next().await.transpose()
    }

    /// For a peer whose oversized message is a protocol violation: the first one ends the stream.
    pub async fn next_line(&mut self) -> std::io::Result<Option<String>> {
        match self.next_frame().await? {
            Some(ProtocolFrame::Line(line)) => Ok(Some(line)),
            Some(ProtocolFrame::Oversized { .. }) => {
                Err(std::io::Error::other(format!("message exceeds {MAX_MESSAGE_BYTES} bytes")))
            }
            None => Ok(None),
        }
    }
}

/// Newline-delimited frames bounded at [`MAX_MESSAGE_BYTES`]. Unlike `LinesCodec`, whose length
/// error ends a `FramedRead`, an oversized message is reported once, as soon as it crosses the
/// limit, and then skipped, so one bad message does not end the connection it arrived on.
#[derive(Default)]
struct BoundedLines {
    /// Bytes already searched for a newline.
    searched: usize,
    discarding: bool,
}

impl Decoder for BoundedLines {
    type Item = ProtocolFrame;
    type Error = std::io::Error;

    fn decode(&mut self, buffer: &mut BytesMut) -> std::io::Result<Option<ProtocolFrame>> {
        loop {
            let newline = buffer[self.searched..].iter().position(|byte| *byte == b'\n').map(|at| self.searched + at);
            if self.discarding {
                match newline {
                    Some(at) => {
                        buffer.advance(at + 1);
                        self.searched = 0;
                        self.discarding = false;
                        continue;
                    }
                    None => {
                        buffer.clear();
                        self.searched = 0;
                        return Ok(None);
                    }
                }
            }
            match newline {
                Some(at) if at <= MAX_MESSAGE_BYTES => {
                    let line = buffer.split_to(at + 1);
                    self.searched = 0;
                    return utf8_line(&line[..at]).map(|line| Some(ProtocolFrame::Line(line)));
                }
                Some(at) => {
                    let head = oversized_head(&buffer[..at]);
                    buffer.advance(at + 1);
                    self.searched = 0;
                    return Ok(Some(head));
                }
                None if buffer.len() > MAX_MESSAGE_BYTES => {
                    let head = oversized_head(buffer);
                    buffer.clear();
                    self.searched = 0;
                    self.discarding = true;
                    return Ok(Some(head));
                }
                None => {
                    self.searched = buffer.len();
                    return Ok(None);
                }
            }
        }
    }

    fn decode_eof(&mut self, buffer: &mut BytesMut) -> std::io::Result<Option<ProtocolFrame>> {
        if let Some(frame) = self.decode(buffer)? {
            return Ok(Some(frame));
        }
        if self.discarding || buffer.is_empty() {
            buffer.clear();
            return Ok(None);
        }
        let line = buffer.split();
        self.searched = 0;
        utf8_line(&line).map(|line| Some(ProtocolFrame::Line(line)))
    }
}

fn utf8_line(bytes: &[u8]) -> std::io::Result<String> {
    let bytes = bytes.strip_suffix(b"\r").unwrap_or(bytes);
    String::from_utf8(bytes.to_vec()).map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))
}

fn oversized_head(bytes: &[u8]) -> ProtocolFrame {
    let head = &bytes[..bytes.len().min(OVERSIZED_HEAD_BYTES)];
    ProtocolFrame::Oversized { head: String::from_utf8_lossy(head).into_owned() }
}

/// The JSON-RPC envelope of a message known only by its start: its id and whether it is a request
/// or notification (`method`) rather than a response. Members after the first one that does not
/// fit in `head` are unknown.
#[derive(Debug, Default, PartialEq)]
pub struct OversizedEnvelope {
    pub id: Option<Value>,
    pub method: bool,
}

pub fn oversized_envelope(head: &str) -> OversizedEnvelope {
    let mut envelope = OversizedEnvelope::default();
    let bytes = head.as_bytes();
    let mut at = skip_space(bytes, 0);
    if bytes.get(at) != Some(&b'{') {
        return envelope;
    }
    at += 1;
    loop {
        at = skip_space(bytes, at);
        let Some(key_end) = string_end(bytes, at) else { return envelope };
        let Ok(key) = serde_json::from_slice::<String>(&bytes[at..key_end]) else { return envelope };
        at = skip_space(bytes, key_end);
        if bytes.get(at) != Some(&b':') {
            return envelope;
        }
        at = skip_space(bytes, at + 1);
        let Some(value_end) = value_end(bytes, at) else { return envelope };
        match key.as_str() {
            "id" => envelope.id = serde_json::from_slice::<Value>(&bytes[at..value_end]).ok(),
            "method" => envelope.method = true,
            _ => {}
        }
        at = skip_space(bytes, value_end);
        if bytes.get(at) != Some(&b',') {
            return envelope;
        }
        at += 1;
    }
}

fn skip_space(bytes: &[u8], mut at: usize) -> usize {
    while bytes.get(at).is_some_and(u8::is_ascii_whitespace) {
        at += 1;
    }
    at
}

/// The index just past the JSON string starting at `at`.
fn string_end(bytes: &[u8], at: usize) -> Option<usize> {
    if bytes.get(at) != Some(&b'"') {
        return None;
    }
    let mut index = at + 1;
    while let Some(byte) = bytes.get(index) {
        match byte {
            b'\\' => index += 2,
            b'"' => return Some(index + 1),
            _ => index += 1,
        }
    }
    None
}

/// The index just past the JSON value starting at `at`, if it ends within `bytes`.
fn value_end(bytes: &[u8], at: usize) -> Option<usize> {
    let mut depth = 0usize;
    let mut index = at;
    loop {
        match *bytes.get(index)? {
            b'"' => {
                index = string_end(bytes, index)?;
                if depth == 0 {
                    return Some(index);
                }
                continue;
            }
            b'{' | b'[' => depth += 1,
            b'}' | b']' => {
                if depth == 0 {
                    return Some(index);
                }
                depth -= 1;
                if depth == 0 {
                    return Some(index + 1);
                }
            }
            b',' if depth == 0 => return Some(index),
            byte if depth == 0 && byte.is_ascii_whitespace() => return Some(index),
            _ => {}
        }
        index += 1;
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
    async fn an_oversized_message_is_reported_once_and_the_next_one_still_arrives() {
        let (mut writer, reader) = tokio::io::duplex(1 << 20);
        let writing = tokio::spawn(async move {
            let mut message = br#"{"jsonrpc":"2.0","id":7,"result":{"data":""#.to_vec();
            message.resize(MAX_MESSAGE_BYTES + 1024, b'x');
            message.extend_from_slice(b"\"}}\n{\"jsonrpc\":\"2.0\",\"id\":8,\"result\":null}\n");
            writer.write_all(&message).await.unwrap();
        });
        let mut lines = protocol_lines(reader);
        let Some(ProtocolFrame::Oversized { head }) = lines.next_frame().await.unwrap() else {
            panic!("expected an oversized frame");
        };
        assert!(head.len() <= OVERSIZED_HEAD_BYTES);
        assert_eq!(oversized_envelope(&head), OversizedEnvelope { id: Some(Value::from(7)), method: false });
        assert_eq!(
            lines.next_frame().await.unwrap(),
            Some(ProtocolFrame::Line(r#"{"jsonrpc":"2.0","id":8,"result":null}"#.into()))
        );
        assert_eq!(lines.next_frame().await.unwrap(), None);
        writing.await.unwrap();
    }

    #[test]
    fn envelopes_are_read_from_the_start_of_a_message() {
        assert_eq!(
            oversized_envelope(r#"{"jsonrpc":"2.0","id":"a\"1","method":"session/update","params":{"x":[1,"#),
            OversizedEnvelope { id: Some(Value::from("a\"1")), method: true }
        );
        assert_eq!(
            oversized_envelope(r#" { "id" : 3 , "result" : {"marketplaces":[{"#),
            OversizedEnvelope { id: Some(Value::from(3)), method: false }
        );
        assert_eq!(
            oversized_envelope(r#"{"jsonrpc":"2.0","method":"session/update","params":{"#),
            OversizedEnvelope { id: None, method: true }
        );
        assert_eq!(oversized_envelope("not json"), OversizedEnvelope::default());
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
