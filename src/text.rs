/// Text processing: streaming sentence splitting + cleanup before TTS
use tracing::warn;

/// Phrases where the user ends the session (matches final clause with common spoken lead-ins)
pub fn is_exit(text: &str) -> bool {
    const SEP: [char; 12] = [
        '。', '！', '？', '!', '?', '.', '，', ',', '、', ' ', '～', '~',
    ];
    const LEAD: [&str; 13] = [
        "好的",
        "好了",
        "行了",
        "没事了",
        "可以",
        "那就",
        "那",
        "好",
        "行",
        "嗯",
        "哦",
        "你",
        "先",
    ];
    const EXIT: [&str; 9] = [
        "退下",
        "退下吧",
        "退下了",
        "再见",
        "再见吧",
        "再见了",
        "拜拜",
        "拜拜啦",
        "拜拜了",
    ];
    let Some(mut tail) = text.split(SEP).rev().find(|s| !s.is_empty()) else {
        return false;
    };
    loop {
        if EXIT.contains(&tail) {
            return true;
        }
        let Some(rest) = LEAD.iter().find_map(|p| tail.strip_prefix(p)) else {
            return false;
        };
        tail = rest;
    }
}

/// Split on Chinese punctuation so text can be synthesized while it is generated
#[allow(dead_code)]
pub fn split_sentences(buf: &mut String, force: bool) -> Vec<String> {
    let mut is_first = false;
    split_sentences_streaming(buf, &mut is_first, force)
}

/// Streaming split: the first sentence may be cut at commas to lower TTFA, later ones break on sentence-final punctuation.
pub fn split_sentences_streaming(buf: &mut String, is_first: &mut bool, force: bool) -> Vec<String> {
    const ENDS: [char; 8] = ['。', '！', '？', '!', '?', '；', ';', '\n'];
    const SOFT: [char; 4] = ['，', ',', '、', '：'];
    const FIRST_MIN_CHARS: usize = 4;
    let mut out = Vec::new();
    loop {
        let pos = if *is_first {
            let mut count = 0usize;
            buf.char_indices().find_map(|(i, c)| {
                if ENDS.contains(&c) || (SOFT.contains(&c) && count >= FIRST_MIN_CHARS) {
                    Some((i, c.len_utf8()))
                } else {
                    count += 1;
                    None
                }
            })
        } else {
            buf.char_indices()
                .find(|(_, c)| ENDS.contains(c))
                .map(|(i, c)| (i, c.len_utf8()))
        };
        match pos {
            Some((i, len)) => {
                let sentence: String = buf.drain(..i + len).collect();
                let s = sentence
                    .trim()
                    .trim_matches(|c: char| ENDS.contains(&c) || SOFT.contains(&c))
                    .to_string();
                if !s.is_empty() {
                    *is_first = false;
                    out.push(s);
                }
            }
            None => break,
        }
    }
    if force {
        let rest = buf
            .trim()
            .trim_matches(|c: char| ENDS.contains(&c) || SOFT.contains(&c))
            .to_string();
        buf.clear();
        if !rest.is_empty() {
            *is_first = false;
            out.push(rest);
        }
    } else if buf.chars().count() >= 40 {
        // No sentence-final punctuation: prefer a comma cut, else cut by length so playback does not stall
        let soft_cut = buf
            .char_indices()
            .enumerate()
            .filter(|(idx, (_, c))| *idx >= 12 && SOFT.contains(c))
            .map(|(_, (i, c))| i + c.len_utf8())
            .last();
        let cut = soft_cut
            .or_else(|| buf.char_indices().nth(30).map(|(i, _)| i))
            .unwrap_or(0);
        if cut > 0 {
            let s: String = buf.drain(..cut).collect();
            let s = s
                .trim()
                .trim_matches(|c: char| ENDS.contains(&c) || SOFT.contains(&c))
                .to_string();
            if !s.is_empty() {
                *is_first = false;
                out.push(s);
            }
        }
    }
    out
}

/// Strip markdown / emoji so TTS does not read symbols aloud
pub fn clean_for_tts(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        match c {
            '*' | '#' | '`' | '~' | '|' | '>' | '[' | ']' | '(' | ')' => {}
            _ => {
                if !is_emoji(c) {
                    out.push(c);
                }
            }
        }
    }
    if out.trim().is_empty() {
        warn!("Skipping empty TTS text");
    }
    out.trim().to_string()
}

fn is_emoji(c: char) -> bool {
    let cp = c as u32;
    (0x1F300..=0x1FAFF).contains(&cp) || (0x2600..=0x27BF).contains(&cp) || (0xFE00..=0xFE0F).contains(&cp)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exit_phrases() {
        assert!(is_exit("退下"));
        assert!(is_exit(" 再见。 "));
        assert!(is_exit("拜拜！"));
        assert!(is_exit("好了，你退下吧。"));
        assert!(is_exit("好，那你退下吧。"));
        assert!(is_exit("反正就是一张卷子，哦，你退下吧。"));
        assert!(is_exit("没事了，你可以退下了"));
        assert!(is_exit("好的再见"));
        assert!(!is_exit("不要退下"));
        assert!(!is_exit("别退下"));
        assert!(!is_exit("再见一次"));
        assert!(!is_exit("明天再见"));
        assert!(!is_exit("今天天气怎么样"));
    }

    #[test]
    fn first_sentence_cuts_on_comma_then_uses_full_stops() {
        let mut buf = "明天厦门是晴天，气温十三到二十三度，不会下雨。适合出门哦".to_string();
        let mut is_first = true;
        let parts = split_sentences_streaming(&mut buf, &mut is_first, false);
        assert!(!is_first);
        assert_eq!(parts, vec!["明天厦门是晴天", "气温十三到二十三度，不会下雨"]);
        assert_eq!(buf, "适合出门哦");
        let tail = split_sentences_streaming(&mut buf, &mut is_first, true);
        assert_eq!(tail, vec!["适合出门哦"]);
        assert!(buf.is_empty());
    }
}
