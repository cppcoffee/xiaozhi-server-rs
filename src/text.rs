/// 文本处理：流式切句 + TTS 前的清洗
use tracing::warn;

/// 用户主动结束会话的说法（整句匹配，标点忽略）
pub fn is_exit(text: &str) -> bool {
    const EXIT: [&str; 4] = ["退下", "退下吧", "再见", "拜拜"];
    let t = text.trim().trim_end_matches(['。', '！', '？', '!', '?', '.', '，', ',', ' ']);
    EXIT.contains(&t)
}

/// 按中文标点切句，用于边生成边合成
#[allow(dead_code)]
pub fn split_sentences(buf: &mut String, force: bool) -> Vec<String> {
    let mut is_first = false;
    split_sentences_streaming(buf, &mut is_first, force)
}

/// 流式切句：首句允许按逗号/顿号快速切分以压低 TTFA，后续按句末标点自然断句。
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
        // 没有句末标点时优先在逗号处切分，否则按长度兜底切分，避免长时间不出声
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

/// 过滤 markdown / emoji，避免 TTS 念出符号
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
        assert!(!is_exit("再见一次"));
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
