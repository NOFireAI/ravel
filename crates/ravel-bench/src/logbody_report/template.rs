//! The template split the report measures: a log body becomes a template
//! (its constant text, with a placeholder byte where each variable stood) plus
//! the variables in order. [`reconstruct`] inverts [`split`] byte for byte;
//! the report checks that on every line and fails the run on any mismatch.

/// Marks a variable's position in a template.
pub const PLACEHOLDER: u8 = 0x00;
/// Prefixes a literal [`PLACEHOLDER`] or [`ESCAPE`] byte of the original body.
pub const ESCAPE: u8 = 0x01;

/// The split rule, as the report states it. [`split`] implements exactly this.
pub const RULE: &str = "A word is a maximal run of ASCII bytes [A-Za-z0-9_]; every other byte \
is literal template text. A word is one variable when it is a hex token: 0x or 0X followed by \
one or more hex digits, or a run of hex digits [0-9a-fA-F] containing at least one decimal \
digit. In any other word, each maximal run of decimal digits [0-9] is one variable and the \
remaining bytes are literal. A variable is replaced by the byte 0x00 and stored verbatim \
(leading zeros and case kept); a literal 0x00 or 0x01 byte is stored as 0x01 followed by \
that byte.";

/// One body split into its template and its variables, in order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Split<'a> {
    pub template: Vec<u8>,
    pub vars: Vec<&'a [u8]>,
}

fn is_word_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// Whether a whole word is a hex token under [`RULE`].
fn is_hex_token(word: &[u8]) -> bool {
    if let [b'0', b'x' | b'X', rest @ ..] = word
        && !rest.is_empty()
        && rest.iter().all(u8::is_ascii_hexdigit)
    {
        return true;
    }
    word.iter().all(u8::is_ascii_hexdigit) && word.iter().any(u8::is_ascii_digit)
}

fn push_literal(template: &mut Vec<u8>, b: u8) {
    if b == PLACEHOLDER || b == ESCAPE {
        template.push(ESCAPE);
    }
    template.push(b);
}

/// Splits `body` under [`RULE`].
pub fn split(body: &[u8]) -> Split<'_> {
    let mut template = Vec::with_capacity(body.len());
    let mut vars: Vec<&[u8]> = Vec::new();
    let mut i = 0;
    while i < body.len() {
        if !is_word_byte(body[i]) {
            push_literal(&mut template, body[i]);
            i += 1;
            continue;
        }
        let start = i;
        while i < body.len() && is_word_byte(body[i]) {
            i += 1;
        }
        let word = &body[start..i];
        if is_hex_token(word) {
            template.push(PLACEHOLDER);
            vars.push(word);
            continue;
        }
        let mut j = 0;
        while j < word.len() {
            if word[j].is_ascii_digit() {
                let d = j;
                while j < word.len() && word[j].is_ascii_digit() {
                    j += 1;
                }
                template.push(PLACEHOLDER);
                vars.push(&word[d..j]);
            } else {
                template.push(word[j]);
                j += 1;
            }
        }
    }
    Split { template, vars }
}

/// Number of variables `template` expects.
pub fn placeholder_count(template: &[u8]) -> usize {
    let mut n = 0;
    let mut i = 0;
    while i < template.len() {
        match template[i] {
            ESCAPE => i += 2,
            PLACEHOLDER => {
                n += 1;
                i += 1;
            }
            _ => i += 1,
        }
    }
    n
}

/// Rebuilds a body from its template and variables. `None` when the template
/// is malformed (a trailing escape) or the variable count disagrees with the
/// template's placeholders.
pub fn reconstruct<V: AsRef<[u8]>>(template: &[u8], vars: &[V]) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(template.len());
    let mut next = vars.iter();
    let mut i = 0;
    while i < template.len() {
        match template[i] {
            ESCAPE => {
                out.push(*template.get(i + 1)?);
                i += 2;
            }
            PLACEHOLDER => {
                out.extend_from_slice(next.next()?.as_ref());
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    if next.next().is_some() {
        return None;
    }
    Some(out)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    fn vars_of(s: &str) -> Vec<String> {
        split(s.as_bytes())
            .vars
            .iter()
            .map(|v| String::from_utf8(v.to_vec()).unwrap())
            .collect()
    }

    #[test]
    fn rule_cases() {
        assert_eq!(
            vars_of("blk_-1608999687919862906 from 10.250.19.102:50010"),
            ["1608999687919862906", "10", "250", "19", "102", "50010"]
        );
        assert_eq!(
            vars_of("ptr 0x7f3A user42 id 3fa9c1 word cafe dead"),
            ["0x7f3A", "42", "3fa9c1"]
        );
        assert_eq!(vars_of("at 09:05:03.007"), ["09", "05", "03", "007"]);
        let s = split(b"user42  ok");
        assert_eq!(s.template, b"user\0  ok");
    }

    #[test]
    fn escapes_round_trip() {
        let body = b"a\x00b\x01c 12";
        let s = split(body);
        assert_eq!(placeholder_count(&s.template), 1);
        assert_eq!(reconstruct(&s.template, &s.vars).unwrap(), body);
    }

    #[test]
    fn reconstruct_refuses_count_mismatch() {
        let s = split(b"x 1 y 2");
        assert!(reconstruct(&s.template, &s.vars[..1]).is_none());
        let mut more = s.vars.clone();
        more.push(b"3");
        assert!(reconstruct(&s.template, &more).is_none());
    }
}
