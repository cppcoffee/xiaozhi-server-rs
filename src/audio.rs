use audiopus::coder::{Decoder, Encoder};
use audiopus::packet::Packet;
use audiopus::{Application, Bitrate, Channels, MutSignals, SampleRate, Signal};

/// Device uplink: 16k mono Opus -> PCM16
pub struct OpusIn {
    dec: Decoder,
    buf: Vec<i16>,
}

impl OpusIn {
    pub fn new(sample_rate: u32) -> anyhow::Result<Self> {
        let sr = match sample_rate {
            8000 => SampleRate::Hz8000,
            12000 => SampleRate::Hz12000,
            16000 => SampleRate::Hz16000,
            24000 => SampleRate::Hz24000,
            _ => SampleRate::Hz48000,
        };
        Ok(Self {
            dec: Decoder::new(sr, Channels::Mono)?,
            buf: vec![0i16; 5760],
        })
    }

    pub fn decode(&mut self, packet: &[u8]) -> anyhow::Result<&[i16]> {
        let pkt = Packet::try_from(packet)?;
        let signals = MutSignals::try_from(&mut self.buf)?;
        let n = self.dec.decode(Some(pkt), signals, false)?;
        Ok(&self.buf[..n])
    }
}

/// Downlink: PCM16 (16k/24k) -> 60ms Opus frames (emits frames while PCM is still arriving)
pub struct OpusOut {
    enc: Encoder,
    frame_samples: usize,
    out_buf: Vec<u8>,
    pending_pcm: Vec<i16>,
    trailing_byte: Option<u8>,
}

impl OpusOut {
    pub fn new(sample_rate: u32, frame_ms: u32) -> anyhow::Result<Self> {
        let sr = match sample_rate {
            8000 => SampleRate::Hz8000,
            12000 => SampleRate::Hz12000,
            16000 => SampleRate::Hz16000,
            24000 => SampleRate::Hz24000,
            _ => SampleRate::Hz48000,
        };
        let frame_samples = (sample_rate as usize * frame_ms as usize) / 1000;
        let mut enc = Encoder::new(sr, Channels::Mono, Application::Audio)?;
        let _ = enc.set_bitrate(Bitrate::BitsPerSecond(24_000));
        let _ = enc.set_signal(Signal::Voice);
        let _ = enc.set_complexity(5);
        Ok(Self {
            enc,
            frame_samples,
            out_buf: vec![0u8; 4000],
            pending_pcm: Vec::with_capacity(frame_samples * 2),
            trailing_byte: None,
        })
    }

    /// Encode arbitrary-length PCM as 60ms frames
    #[allow(dead_code)]
    pub fn encode(&mut self, pcm: &[i16]) -> Vec<Vec<u8>> {
        let mut frames = Vec::new();
        for chunk in pcm.chunks_exact(self.frame_samples) {
            if let Ok(n) = self.enc.encode(chunk, &mut self.out_buf) {
                frames.push(self.out_buf[..n].to_vec());
            }
        }
        frames
    }

    /// Zero-pad to a full frame, then encode (used at end of utterance)
    #[allow(dead_code)]
    pub fn encode_padded(&mut self, pcm: &[i16]) -> Vec<Vec<u8>> {
        let mut frames = self.encode(pcm);
        let rest = pcm.len() % self.frame_samples;
        if rest != 0 {
            let mut tail = vec![0i16; self.frame_samples];
            tail[..rest].copy_from_slice(&pcm[pcm.len() - rest..]);
            if let Ok(n) = self.enc.encode(&tail, &mut self.out_buf) {
                frames.push(self.out_buf[..n].to_vec());
            }
        }
        frames
    }

    /// Feed little-endian PCM16 bytes; encode and return each completed frame
    pub fn push_pcm_bytes(&mut self, mut bytes: &[u8]) -> Vec<Vec<u8>> {
        if let Some(lo) = self.trailing_byte.take() {
            if let Some((&hi, rest)) = bytes.split_first() {
                self.pending_pcm.push(i16::from_le_bytes([lo, hi]));
                bytes = rest;
            } else {
                self.trailing_byte = Some(lo);
                return Vec::new();
            }
        }
        let pairs = bytes.len() / 2;
        self.pending_pcm.reserve(pairs);
        for pair in bytes[..pairs * 2].chunks_exact(2) {
            self.pending_pcm.push(i16::from_le_bytes([pair[0], pair[1]]));
        }
        if bytes.len() % 2 == 1 {
            self.trailing_byte = Some(bytes[bytes.len() - 1]);
        }
        let mut frames = Vec::new();
        let full_samples = (self.pending_pcm.len() / self.frame_samples) * self.frame_samples;
        if full_samples > 0 {
            for chunk in self.pending_pcm[..full_samples].chunks_exact(self.frame_samples) {
                if let Ok(n) = self.enc.encode(chunk, &mut self.out_buf) {
                    frames.push(self.out_buf[..n].to_vec());
                }
            }
            self.pending_pcm.drain(..full_samples);
        }
        frames
    }

    /// End-of-utterance flush: zero-pad the remaining samples into one frame
    pub fn finish_stream(&mut self) -> Option<Vec<u8>> {
        self.trailing_byte = None;
        if self.pending_pcm.is_empty() {
            return None;
        }
        self.pending_pcm.resize(self.frame_samples, 0);
        let frame = self
            .enc
            .encode(&self.pending_pcm[..self.frame_samples], &mut self.out_buf)
            .ok()
            .map(|n| self.out_buf[..n].to_vec());
        self.pending_pcm.clear();
        frame
    }
}

#[allow(dead_code)]
pub fn bytes_to_i16(bytes: &[u8]) -> Vec<i16> {
    bytes
        .chunks_exact(2)
        .map(|c| i16::from_le_bytes([c[0], c[1]]))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sine(sample_rate: u32, ms: u32) -> Vec<i16> {
        let n = (sample_rate as usize * ms as usize) / 1000;
        (0..n)
            .map(|i| {
                let t = i as f32 / sample_rate as f32;
                ((t * 440.0 * 2.0 * std::f32::consts::PI).sin() * 8000.0) as i16
            })
            .collect()
    }

    #[test]
    fn opus_roundtrip_24k() {
        let mut enc = OpusOut::new(24000, 60).unwrap();
        let frames = enc.encode(&sine(24000, 120));
        assert_eq!(frames.len(), 2, "120ms 应切成 2 个 60ms 帧");
        let mut dec = OpusIn::new(24000).unwrap();
        let out = dec.decode(&frames[0]).unwrap();
        assert!(out.len() >= 1400 && out.len() <= 1460, "解出样本数 {}", out.len());
    }

    #[test]
    fn opus_roundtrip_16k() {
        let mut enc = OpusOut::new(16000, 60).unwrap();
        let frames = enc.encode(&sine(16000, 60));
        assert_eq!(frames.len(), 1);
        let mut dec = OpusIn::new(16000).unwrap();
        assert_eq!(dec.decode(&frames[0]).unwrap().len(), 960);
    }

    #[test]
    fn streaming_push_pcm_bytes_handles_unaligned_chunks_and_tail() {
        let mut enc = OpusOut::new(16000, 60).unwrap();
        // 1.5 frames (1440 samples = 2880 bytes), split across odd-sized chunks.
        let pcm = sine(16000, 90);
        let bytes: Vec<u8> = pcm.iter().flat_map(|s| s.to_le_bytes()).collect();
        let mut frames = Vec::new();
        for chunk in bytes.chunks(333) {
            frames.extend(enc.push_pcm_bytes(chunk));
        }
        assert_eq!(frames.len(), 1, "first 60ms frame should emit before flush");
        let tail = enc.finish_stream().expect("remaining 30ms should zero-pad");
        frames.push(tail);
        assert_eq!(frames.len(), 2);
        let mut dec = OpusIn::new(16000).unwrap();
        for frame in &frames {
            assert_eq!(dec.decode(frame).unwrap().len(), 960);
        }
    }
}
