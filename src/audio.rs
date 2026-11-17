use audiopus::coder::{Decoder, Encoder};
use audiopus::packet::Packet;
use audiopus::{Application, Channels, MutSignals, SampleRate};

/// 设备上行：16k 单声道 Opus → PCM16
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

/// 下行：PCM16（24k）→ Opus 60ms 帧
pub struct OpusOut {
    enc: Encoder,
    frame_samples: usize,
    out_buf: Vec<u8>,
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
        Ok(Self {
            enc: Encoder::new(sr, Channels::Mono, Application::Audio)?,
            frame_samples,
            out_buf: vec![0u8; 4000],
        })
    }

    /// 把任意长度的 PCM 切成 60ms 帧逐帧编码
    pub fn encode(&mut self, pcm: &[i16]) -> Vec<Vec<u8>> {
        let mut frames = Vec::new();
        for chunk in pcm.chunks_exact(self.frame_samples) {
            if let Ok(n) = self.enc.encode(chunk, &mut self.out_buf) {
                frames.push(self.out_buf[..n].to_vec());
            }
        }
        frames
    }

    /// 补零到整帧后再编码（句尾用）
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
}

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

}
