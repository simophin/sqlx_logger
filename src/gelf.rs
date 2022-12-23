use std::{
    borrow::Cow,
    cmp::Ordering,
    collections::HashMap,
    time::{Duration, Instant},
};

use anyhow::{bail, Context};
use bytes::Buf;

const CHUNKED_MAGIC_BYTES: &[u8] = &[0x1e, 0x0f];
const MAX_CHUNKED_MESSAGE_DURATION: Duration = Duration::from_secs(120);

type MessageID = [u8; 8];
type ChunkSeq = u8;

struct MergeChunk {
    start: ChunkSeq,
    end: ChunkSeq,
    data: Vec<u8>,
}

struct MessageState {
    first_arrived: Instant,
    total_seq: ChunkSeq,
    sorted_chunks: Vec<MergeChunk>,
}

#[derive(Default)]
pub struct GELFState {
    messages: HashMap<MessageID, MessageState>,
}

fn data_to_str(data: Cow<[u8]>) -> anyhow::Result<Option<Cow<str>>> {
    match data {
        Cow::Borrowed(v) => std::str::from_utf8(v)
            .context("Converting data into UTF-8 string")
            .map(|v| Some(Cow::Borrowed(v))),
        Cow::Owned(v) => String::from_utf8(v)
            .context("Converting data into UTF-8 string")
            .map(|v| Some(Cow::Owned(v))),
    }
}

impl GELFState {
    pub fn on_data<'a>(&mut self, mut data: &'a [u8]) -> anyhow::Result<Option<Cow<'a, str>>> {
        if data.starts_with(CHUNKED_MAGIC_BYTES) {
            if data.len() < 12 {
                bail!(
                    "Invalid chunked header. Expecting header to be at least 12 but got: {}",
                    data.len()
                );
            }

            data.advance(CHUNKED_MAGIC_BYTES.len()); // The magic bytes

            let mut id: MessageID = Default::default();
            data.copy_to_slice(&mut id);

            let seq: ChunkSeq = data.get_u8();
            let total_seq: ChunkSeq = data.get_u8();

            match total_seq {
                0 => bail!("Total seq is 0"),
                1 => return data_to_str(Cow::Borrowed(data)),
                _ => {}
            }

            let state = self
                .messages
                .entry(id.clone())
                .or_insert_with(|| MessageState {
                    first_arrived: Instant::now(),
                    total_seq,
                    sorted_chunks: Default::default(),
                });

            let rs = state.try_merge(seq, data);

            if matches!(&rs, Ok(Some(_))) {
                self.messages.remove(&id);
            }

            rs
        } else {
            data_to_str(Cow::Borrowed(data))
        }
    }

    pub fn clean_up(&mut self, now: Instant) {
        self.messages
            .retain(|_, item| now - item.first_arrived < MAX_CHUNKED_MESSAGE_DURATION);
    }
}

fn compare_chunk(chunk: &MergeChunk, seq: ChunkSeq) -> Ordering {
    if chunk.start > seq {
        Ordering::Greater
    } else if chunk.end < seq {
        Ordering::Less
    } else {
        Ordering::Equal
    }
}

impl MessageState {
    fn try_merge<'a>(
        &mut self,
        seq: ChunkSeq,
        data: &'a [u8],
    ) -> anyhow::Result<Option<Cow<'a, str>>> {
        // Find the closest chunk
        match self
            .sorted_chunks
            .binary_search_by(|probe| compare_chunk(probe, seq))
        {
            Ok(_) => {
                // This indicates this seq is already in one of our chunk: ignoring...
                log::debug!("Ignoring chunk seq = {seq}: already in the merge chunk");
                return Ok(None);
            }
            Err(index) => {
                if index == 0 {
                    // There's no place to insert our data into, just create a new merge chunk
                    self.sorted_chunks.insert(
                        0,
                        MergeChunk {
                            start: seq,
                            end: seq,
                            data: data.to_vec(),
                        },
                    );
                } else {
                    let last_chunk = self.sorted_chunks.get_mut(index - 1).unwrap();
                    if last_chunk.end + 1 == seq {
                        // This means we can just merge the data into last chunk instead of creating a new one
                        last_chunk.data.extend_from_slice(data);
                        last_chunk.end += 1;
                    } else {
                        // This means this data has to create its own chunk
                        self.sorted_chunks.insert(
                            index,
                            MergeChunk {
                                start: seq,
                                end: seq,
                                data: data.to_vec(),
                            },
                        );
                    }
                }
            }
        }

        // Check if we have full data
        if self.sorted_chunks.last().unwrap().end == self.total_seq - 1
            && self.sorted_chunks.first().unwrap().start == 0
            && self.are_chunks_continuous()
        {
            let num_total_bytes = self
                .sorted_chunks
                .iter()
                .fold(0usize, |acc, item| acc + item.data.len());

            let mut data = Vec::with_capacity(num_total_bytes);
            for chunk in &self.sorted_chunks {
                data.extend_from_slice(&chunk.data);
            }

            self.sorted_chunks.clear();
            data_to_str(Cow::Owned(data))
        } else {
            Ok(None)
        }
    }

    fn are_chunks_continuous(&self) -> bool {
        let mut last: Option<&MergeChunk> = None;
        for chunk in &self.sorted_chunks {
            match &last {
                Some(v) if chunk.start - v.end > 1 => return false,
                _ => {}
            }

            last.replace(chunk);
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unchunked_data() {
        let mut state = GELFState::default();
        let expect = "hello, world";
        let actual = state
            .on_data(expect.as_bytes())
            .expect("No error")
            .expect("Some message");
        assert_eq!(expect, actual.as_ref());
    }

    fn new_chunk_message(id: &MessageID, seq: u8, total: u8, msg: impl AsRef<[u8]>) -> Vec<u8> {
        let mut input = vec![];
        input.extend_from_slice(CHUNKED_MAGIC_BYTES);
        input.extend_from_slice(id);
        input.push(seq);
        input.push(total);
        input.extend_from_slice(msg.as_ref());
        input
    }

    #[test]
    fn chunked_once() {
        let mut state = GELFState::default();
        let expect = "hello, world";

        let input = new_chunk_message(&Default::default(), 0, 1, expect);

        let actual = state
            .on_data(&input)
            .expect("No error")
            .expect("Some message");
        assert_eq!(expect, actual.as_ref());
    }

    fn test_chunked(input_message: &str, num_total: u8, chunks: &[(u8, &str)]) {
        let mut state = GELFState::default();

        let id: MessageID = Default::default();

        for (seq, data) in chunks.iter().take(chunks.len() - 1) {
            let input = new_chunk_message(&id, *seq, num_total, data);
            let rs = state.on_data(&input);

            assert!(matches!(rs, Ok(None)));
        }

        let last_chunk = chunks.last().unwrap();

        let input = new_chunk_message(&id, last_chunk.0, chunks.len() as u8, last_chunk.1);
        let actual = state
            .on_data(&input)
            .expect("No error")
            .expect("Some message");
        assert_eq!(input_message, actual.as_ref());
    }

    #[test]
    fn chunked_continuously() {
        let expect = "123456789";
        test_chunked(
            expect,
            3,
            &[(0u8, &expect[0..3]), (1, &expect[3..6]), (2, &expect[6..9])],
        );
    }

    #[test]
    fn chunked_with_disorders() {
        let expect = "123456789";
        test_chunked(
            expect,
            3,
            &[(0, &expect[0..3]), (2, &expect[6..9]), (1, &expect[3..6])],
        );
    }

    #[test]
    fn chunked_with_duplications() {
        let expect = "123456789";
        test_chunked(
            expect,
            3,
            &[
                (0, &expect[0..3]),
                (1, &expect[3..6]),
                (1, &expect[3..6]),
                (2, &expect[6..9]),
            ],
        );
    }

    #[test]
    fn chunked_with_disorders_and_duplications() {
        let expect = "123456789";
        test_chunked(
            expect,
            3,
            &[
                (2, &expect[6..9]),
                (1, &expect[3..6]),
                (1, &expect[3..6]),
                (0, &expect[0..3]),
            ],
        );
    }

    #[test]
    fn chunked_with_timeout() {
        let mut state = GELFState::default();

        let message = "123456789";
        let id: MessageID = Default::default();

        let input = new_chunk_message(&id, 0, 2, &message[..4]);
        let output = state.on_data(&input);
        assert!(matches!(output, Ok(None)));

        state.clean_up(Instant::now() + MAX_CHUNKED_MESSAGE_DURATION + Duration::from_secs(1));

        let input = new_chunk_message(&id, 1, 2, &message[4..]);
        let output = state.on_data(&input);
        assert!(matches!(output, Ok(None)));
    }
}
