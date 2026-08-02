//! Word tokenizer for bloom keys and word/phrase match
//! (docs/log-segment-format.md "Tokenizer").
//!
//! This is a normative part of the format: it defines word semantics
//! identically on the write path (bloom insertion) and the read path (word
//! match), so the two can never disagree about what a "word" is.

/// Tokens of `text`: lowercase words split on any non-alphanumeric character.
///
/// - split on `!char::is_alphanumeric` (Unicode-aware);
/// - lowercase each token with `char::to_lowercase` (Unicode-aware);
/// - truncate each token to its longest character-boundary prefix of at most
///   64 bytes;
/// - drop empty tokens; keep duplicates (deduplication is the bloom's job).
pub fn tokens(text: &str) -> Vec<Vec<u8>> {
    /// Byte cap on a single token (docs/log-segment-format.md constant).
    const TOKEN_MAX_BYTES: usize = 64;

    let mut out = Vec::new();
    let mut cur = String::new();
    // `truncated` remembers we have already hit the 64-byte cap for the
    // current word, so the rest of the word's characters are consumed as
    // separators-free continuation but not appended.
    let mut truncated = false;

    let mut flush = |cur: &mut String, truncated: &mut bool| {
        if !cur.is_empty() {
            out.push(std::mem::take(cur).into_bytes());
        } else {
            cur.clear();
        }
        *truncated = false;
    };

    for ch in text.chars() {
        if ch.is_alphanumeric() {
            if truncated {
                continue;
            }
            for lc in ch.to_lowercase() {
                // Appending this lowercased char must keep the token within
                // the 64-byte cap on a character boundary.
                if cur.len() + lc.len_utf8() > TOKEN_MAX_BYTES {
                    truncated = true;
                    break;
                }
                cur.push(lc);
            }
        } else {
            flush(&mut cur, &mut truncated);
        }
    }
    flush(&mut cur, &mut truncated);
    out
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn toks(s: &str) -> Vec<Vec<u8>> {
        tokens(s)
    }

    #[test]
    fn splits_and_lowercases() {
        assert_eq!(
            toks("GET /api/v1?x=1 timeout"),
            vec![
                b"get".to_vec(),
                b"api".to_vec(),
                b"v1".to_vec(),
                b"x".to_vec(),
                b"1".to_vec(),
                b"timeout".to_vec(),
            ]
        );
    }

    #[test]
    fn truncates_at_64_bytes_on_char_boundary() {
        let long = "a".repeat(100);
        assert_eq!(tokens(&long)[0].len(), 64);
        // 'é' is two UTF-8 bytes; the prefix must land on a char boundary and
        // never exceed 64 bytes.
        let multi = "é".repeat(64);
        assert!(tokens(&multi)[0].len() <= 64);
        assert_eq!(tokens(&multi)[0].len() % 2, 0);
    }

    #[test]
    fn drops_empties_and_keeps_duplicates() {
        assert_eq!(toks("  ,, .. "), Vec::<Vec<u8>>::new());
        assert_eq!(
            toks("a a a"),
            vec![b"a".to_vec(), b"a".to_vec(), b"a".to_vec()]
        );
    }

    #[test]
    fn unicode_word_is_lowercased() {
        // A single uppercase multi-byte word lowercases and stays one token.
        assert_eq!(toks("STRASSE"), vec![b"strasse".to_vec()]);
    }

    /// Acceptance test (issue #429): pins the four normative rules from
    /// docs/log-segment-format.md "Tokenizer" in one place, independent of
    /// the smaller unit tests above.
    #[test]
    fn tokens_match_the_rlog_normative_definition() {
        // Rule 1 (split on any non-alphanumeric char, Unicode-aware) and
        // rule 4 (drop empty tokens, keep duplicates).
        assert_eq!(
            toks("GET /api/v1?x=1, timeout timeout"),
            vec![
                b"get".to_vec(),
                b"api".to_vec(),
                b"v1".to_vec(),
                b"x".to_vec(),
                b"1".to_vec(),
                b"timeout".to_vec(),
                b"timeout".to_vec(),
            ]
        );
        assert_eq!(toks("   ,,..  "), Vec::<Vec<u8>>::new());

        // Rule 1 again, with non-ASCII separators. The assertions above use
        // only ASCII punctuation, so on their own they would also pass for an
        // implementation that split on a hardcoded ASCII punctuation set and
        // treated every non-ASCII character as a word character. These pin
        // `char::is_alphanumeric` specifically: U+00B7 MIDDLE DOT and U+3000
        // IDEOGRAPHIC SPACE are both non-ASCII and both non-alphanumeric.
        assert_eq!(toks("a\u{00b7}b"), vec![b"a".to_vec(), b"b".to_vec()]);
        assert_eq!(
            toks("\u{03b1}\u{3000}\u{03b2}"),
            vec![
                "\u{03b1}".as_bytes().to_vec(),
                "\u{03b2}".as_bytes().to_vec()
            ]
        );

        // Rule 2: lowercase each token, Unicode-aware (`char::to_lowercase`,
        // not an ASCII-only lowercasing). Greek and Cyrillic letters
        // lowercase to their own multi-byte lowercase codepoints.
        assert_eq!(toks("ΑΒΓ"), vec!["αβγ".as_bytes().to_vec()]);
        assert_eq!(toks("МОСКВА"), vec!["москва".as_bytes().to_vec()]);

        // Rule 3: truncate to the longest character-boundary prefix of at
        // most 64 bytes. '世' is 3 bytes in UTF-8, so 64 is not a multiple
        // of its width: naive truncation at raw byte offset 64 would land 1
        // byte into the 22nd character (21 * 3 = 63, 22 * 3 = 66) and split
        // it. The tokenizer must instead stop at the last whole character,
        // 21 of them (63 bytes), never producing invalid UTF-8.
        let long = "世".repeat(30);
        let toks = toks(&long);
        assert_eq!(toks.len(), 1);
        assert!(toks[0].len() <= 64);
        assert_eq!(toks[0].len(), 63);
        let s = std::str::from_utf8(&toks[0])
            .expect("truncation must stay on a char boundary, never splitting a codepoint");
        assert_eq!(s.chars().count(), 21);
        assert_eq!(s, "世".repeat(21));
    }
}

/// Every token is at most 64 bytes, valid UTF-8, and contains no
/// non-alphanumeric character.
#[cfg(test)]
#[allow(clippy::expect_used)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn token_invariants(text in ".*") {
            for tok in tokens(&text) {
                prop_assert!(tok.len() <= 64);
                let s = std::str::from_utf8(&tok).expect("valid utf-8");
                prop_assert!(!s.is_empty());
                prop_assert!(s.chars().all(|c| c.is_alphanumeric()));
            }
        }
    }
}
