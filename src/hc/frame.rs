//! herdr-core framed-mode codec: `[channel u32 LE][kind u8][len u32 LE][payload]`.

use std::io::{self, Read, Write};

pub(crate) const CONTROL_CHANNEL: u32 = 0;
pub(crate) const MAX_PAYLOAD: usize = 16 * 1024 * 1024;
const HEADER_LEN: usize = 9;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FrameKind {
    Data,
    Credit,
    Close,
    Resize,
    Focus,
    Facts,
}

impl FrameKind {
    fn to_u8(self) -> u8 {
        match self {
            Self::Data => 0,
            Self::Credit => 1,
            Self::Close => 2,
            Self::Resize => 3,
            Self::Focus => 4,
            Self::Facts => 5,
        }
    }

    fn from_u8(value: u8) -> Option<Self> {
        Some(match value {
            0 => Self::Data,
            1 => Self::Credit,
            2 => Self::Close,
            3 => Self::Resize,
            4 => Self::Focus,
            5 => Self::Facts,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Frame {
    pub channel: u32,
    pub kind: FrameKind,
    pub payload: Vec<u8>,
}

pub(crate) fn encode(channel: u32, kind: FrameKind, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER_LEN + payload.len());
    out.extend_from_slice(&channel.to_le_bytes());
    out.push(kind.to_u8());
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(payload);
    out
}

#[cfg(test)]
pub(crate) fn encode_credit(channel: u32, bytes: u32) -> Vec<u8> {
    encode(channel, FrameKind::Credit, &bytes.to_le_bytes())
}

#[cfg(test)]
pub(crate) fn encode_resize(channel: u32, cols: u16, rows: u16) -> Vec<u8> {
    let mut payload = [0u8; 4];
    payload[..2].copy_from_slice(&cols.to_le_bytes());
    payload[2..].copy_from_slice(&rows.to_le_bytes());
    encode(channel, FrameKind::Resize, &payload)
}

pub(crate) fn write_frame(
    writer: &mut impl Write,
    channel: u32,
    kind: FrameKind,
    payload: &[u8],
) -> io::Result<()> {
    writer.write_all(&encode(channel, kind, payload))
}

/// Reads one frame. `Ok(None)` is a clean end of stream at a frame boundary.
pub(crate) fn read_frame(reader: &mut impl Read) -> io::Result<Option<Frame>> {
    let mut header = [0u8; HEADER_LEN];
    let mut filled = 0;
    while filled < HEADER_LEN {
        match reader.read(&mut header[filled..]) {
            Ok(0) if filled == 0 => return Ok(None),
            Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
            Ok(n) => filled += n,
            Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
            Err(err) => return Err(err),
        }
    }
    let channel = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
    let kind = FrameKind::from_u8(header[4]).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown hc frame kind {}", header[4]),
        )
    })?;
    let len = u32::from_le_bytes([header[5], header[6], header[7], header[8]]) as usize;
    if len > MAX_PAYLOAD {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("hc frame payload of {len} bytes exceeds the limit"),
        ));
    }
    let mut payload = vec![0u8; len];
    reader.read_exact(&mut payload)?;
    Ok(Some(Frame {
        channel,
        kind,
        payload,
    }))
}

/// Socket buffer for one attach stream; large enough to hold many small frames.
pub(crate) const READ_BUFFER: usize = 256 * 1024;

/// Appends the payloads of DATA frames for `channel` that are already fully
/// buffered, so one parse handles a burst the core split into small frames.
/// Stops before any other frame, an incomplete frame, or `limit` bytes. Never
/// blocks: it only looks at bytes the reader already holds.
pub(crate) fn coalesce_buffered_data(
    reader: &mut io::BufReader<impl Read>,
    channel: u32,
    out: &mut Vec<u8>,
    limit: usize,
) {
    loop {
        let buffered = reader.buffer();
        if buffered.len() < HEADER_LEN {
            return;
        }
        let frame_channel =
            u32::from_le_bytes([buffered[0], buffered[1], buffered[2], buffered[3]]);
        let len = u32::from_le_bytes([buffered[5], buffered[6], buffered[7], buffered[8]]) as usize;
        if frame_channel != channel
            || FrameKind::from_u8(buffered[4]) != Some(FrameKind::Data)
            || buffered.len() < HEADER_LEN + len
            || out.len() + len > limit
        {
            return;
        }
        out.extend_from_slice(&buffered[HEADER_LEN..HEADER_LEN + len]);
        io::BufRead::consume(reader, HEADER_LEN + len);
    }
}

#[cfg(test)]
pub(crate) fn decode_resize(payload: &[u8]) -> Option<(u16, u16)> {
    let bytes: [u8; 4] = payload.try_into().ok()?;
    Some((
        u16::from_le_bytes([bytes[0], bytes[1]]),
        u16::from_le_bytes([bytes[2], bytes[3]]),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_round_trip() {
        let mut wire = encode(7, FrameKind::Data, b"hello");
        wire.extend(encode_credit(7, 5));
        wire.extend(encode_resize(7, 120, 40));
        let mut cursor = std::io::Cursor::new(wire);
        let data = read_frame(&mut cursor).unwrap().unwrap();
        assert_eq!(
            (data.channel, data.kind, data.payload.as_slice()),
            (7, FrameKind::Data, &b"hello"[..])
        );
        let credit = read_frame(&mut cursor).unwrap().unwrap();
        assert_eq!(credit.kind, FrameKind::Credit);
        assert_eq!(credit.payload, 5u32.to_le_bytes());
        let resize = read_frame(&mut cursor).unwrap().unwrap();
        assert_eq!(decode_resize(&resize.payload), Some((120, 40)));
        assert!(read_frame(&mut cursor).unwrap().is_none());
    }

    #[test]
    fn buffered_data_frames_coalesce_up_to_other_frames_and_the_limit() {
        let mut wire = encode(3, FrameKind::Data, b"ab");
        wire.extend(encode(3, FrameKind::Data, b"cd"));
        wire.extend(encode(3, FrameKind::Data, b"ef"));
        wire.extend(encode(9, FrameKind::Data, b"other"));
        wire.extend(encode(3, FrameKind::Data, b"gh"));
        wire.extend(encode(3, FrameKind::Close, b""));
        let mut reader = io::BufReader::new(std::io::Cursor::new(wire));

        let first = read_frame(&mut reader).unwrap().unwrap();
        let mut batch = first.payload;
        coalesce_buffered_data(&mut reader, 3, &mut batch, 64);
        assert_eq!(batch, b"abcdef");

        // A frame for another channel ends the batch and is read normally.
        assert_eq!(read_frame(&mut reader).unwrap().unwrap().channel, 9);
        let next = read_frame(&mut reader).unwrap().unwrap();
        let mut batch = next.payload;
        coalesce_buffered_data(&mut reader, 3, &mut batch, 64);
        assert_eq!(batch, b"gh");
        assert_eq!(
            read_frame(&mut reader).unwrap().unwrap().kind,
            FrameKind::Close
        );
    }

    #[test]
    fn coalescing_respects_the_limit_and_partial_frames() {
        let mut wire = encode(1, FrameKind::Data, b"1234");
        wire.extend(encode(1, FrameKind::Data, b"5678"));
        let mut reader = io::BufReader::new(std::io::Cursor::new(wire.clone()));
        let mut batch = read_frame(&mut reader).unwrap().unwrap().payload;
        coalesce_buffered_data(&mut reader, 1, &mut batch, 6);
        assert_eq!(batch, b"1234", "next frame would exceed the limit");

        // Only part of the second frame has arrived: leave it for a blocking read.
        let partial = &wire[..wire.len() - 2];
        let mut reader = io::BufReader::new(std::io::Cursor::new(partial));
        let mut batch = read_frame(&mut reader).unwrap().unwrap().payload;
        coalesce_buffered_data(&mut reader, 1, &mut batch, 64);
        assert_eq!(batch, b"1234");
    }

    #[test]
    fn truncated_frame_is_an_error_not_a_clean_end() {
        let wire = encode(1, FrameKind::Data, b"abcdef");
        let mut cursor = std::io::Cursor::new(&wire[..wire.len() - 2]);
        assert!(read_frame(&mut cursor).is_err());
        let mut header_only = std::io::Cursor::new(&wire[..4]);
        assert!(read_frame(&mut header_only).is_err());
    }

    #[test]
    fn oversized_and_unknown_frames_are_rejected() {
        let mut wire = encode(1, FrameKind::Data, b"");
        wire[5..9].copy_from_slice(&((MAX_PAYLOAD as u32) + 1).to_le_bytes());
        assert!(read_frame(&mut std::io::Cursor::new(wire)).is_err());
        let mut unknown = encode(1, FrameKind::Data, b"");
        unknown[4] = 99;
        assert!(read_frame(&mut std::io::Cursor::new(unknown)).is_err());
    }
}
