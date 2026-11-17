/// 文本处理：流式切句 + TTS 前的清洗
use tracing::warn;

/// 用户主动结束会话的说法（整句匹配，标点忽略）
pub fn is_exit(text: &str) -> bool {
    const EXIT: [&str; 4] = ["退下", "退下吧", "再见", "拜拜"];
    let t = text.trim().trim_end_matches(['。', '！', '？', '!', '?', '.', '，', ',', ' ']);
    EXIT.contains(&t)
}

/// 按中文标点切句，用于边生成边合成
pub fn split_sentences(buf: &mut String, force: bool) -> Vec<String> {
    const ENDS: [char; 8] = ['。', '！', '？', '!', '?', '；', ';', '\n'];
    let mut out = Vec::new();
    loop {
        let pos = buf.char_indices().find(|(_, c)| ENDS.contains(c)).map(|(i, c)| (i, c.len_utf8()));
        match pos {
            Some((i, len)) => {
                let sentence: String = buf.drain(..i + len).collect();
                let s = sentence.trim().trim_matches(|c: char| ENDS.contains(&c) || c == '，').to_string();
                if !s.is_empty() {
                    out.push(s);
                }
            }
            None => break,
        }
    }
    if force {
        let rest = buf.trim().to_string();
        buf.clear();
        if !rest.is_empty() {
            out.push(rest);
        }
    } else if buf.chars().count() >= 40 {
        // 没有标点时按长度兜底切分，避免长时间不出声
        let cut = buf.char_indices().nth(30).map(|(i, _)| i).unwrap_or(0);
        if cut > 0 {
            let s: String = buf.drain(..cut).collect();
            let s = s.trim().to_string();
            if !s.is_empty() {
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
        warn!("TTS 文本为空，跳过");
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
}
