//! `deploy/prometheus/ravel.rules.yaml` ships alert rules an operator loads
//! verbatim, and `deploy/grafana/dashboards/ravel.json` ships the dashboard
//! the same operator imports. Every Ravel metric name in either must name a
//! series Ravel really renders, or the rule matches nothing and the condition
//! it covers pages nobody, and the panel draws an empty graph that reads as a
//! quiet subsystem, both with no error at scrape time.
//!
//! The assertion is against a RENDERED `/metrics` body, obtained the way
//! `metrics_endpoint.rs` obtains one (an in-process server on `MemoryStore`,
//! scraped over HTTP), not against the string literals in
//! `services/ravel-server/src/metrics.rs`: a name that is declared in the
//! source but that nothing renders would pass a source scan and still leave
//! the shipped rule dead.
//!
//! Both tests here read the rule file through [`parse_rule_file`], which
//! builds its groups and rules as a structure and refuses any line it cannot
//! account for, rather than matching strings in the raw text. A corruption
//! that leaves the `- alert:` lines intact holds a string count at 31 while
//! Prometheus refuses the whole file and every alert in it goes dead, so the
//! count has to come off a parse to mean anything.
//!
//! The parse covers STRUCTURE, not the values inside `expr:` and `for:`. It
//! refuses a line it cannot account for and a key it does not know, but it
//! reads a plain scalar as text: `for: 10 minutes` and an unbalanced bracket
//! inside an `expr` block scalar both parse here and are both refused by
//! Prometheus on load. Issue #1928 covers validating those two fields.
//!
//! `docs/guides/observability.md` reprints 13 of these rules in fenced `yaml`
//! blocks, to explain them in place. Those blocks are parsed the same way and
//! compared field by field against the shipped file, which is what keeps a
//! rename from updating one copy and leaving the other handing readers a dead
//! rule.
//!
//! The dashboard is read by parsing its JSON and walking every object that
//! carries a `targets` array, then extracting metric names from each target's
//! PromQL with [`target_metric_names`]. A panel target is an expression, not
//! a bare name, so the scan runs over that expression with its strings and
//! its template variables blanked out. Everything below the extraction (the
//! `ravel_*` token scanner, the rendered bodies, the `# TYPE` reading) is the
//! same code the rule file goes through: two extractors is how two shipped
//! files drift apart.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use ravel_object_store::StoreMetrics;
use ravel_object_store::memory::MemoryStore;
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::TenantId;

const TOKEN: &str = "testtoken";

/// The shipped rule file, relative to this crate's manifest directory.
const RULES_FILE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../deploy/prometheus/ravel.rules.yaml"
);

/// The guide that reprints part of the shipped file in fenced `yaml` blocks.
const GUIDE_FILE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../docs/guides/observability.md"
);

/// Distinct `ravel_*` metric names the shipped rules reference, over every
/// parsed `expr` and every parsed annotation value.
///
/// Annotations count because one name reaches the set through them alone
/// (`ravel_declared_stats_drops_observed_total`, named by the annotation that
/// tells the responder where to look): a scan of the expressions by
/// themselves would drop it, and a rename of it would then go unnoticed.
///
/// A literal, not a figure derived from the same scan the assertions run over:
/// an extractor that silently matches nothing (a broken pattern, a path that
/// no longer resolves) would otherwise leave every assertion below passing
/// vacuously over an empty set. Adding a rule that names a new metric fails
/// here until this number is updated.
const EXPECTED_METRIC_NAMES: usize = 37;

/// Groups and alert rules in the shipped file, counted off the parsed
/// structure. Pinned for the same reason as the name count: a file that lost
/// a group, or a group that lost a rule, must fail rather than shrink the
/// scan. `deploy/README.md` states both figures.
const EXPECTED_GROUPS: usize = 8;
const EXPECTED_ALERTS: usize = 31;

/// Rules transcribed from a troubleshooting-table row that states no
/// duration, so they carry no `for:` and say so in an `as_documented`
/// annotation. `deploy/README.md` states this figure too.
const EXPECTED_ALERTS_WITHOUT_FOR: usize = 15;

/// The shipped Grafana dashboard over Ravel's own families.
///
/// `deploy/grafana/dashboards/ravel-overview.json` is deliberately not read
/// here: that one belongs to the quickstart and graphs the bundled
/// collector's host CPU, which is not a Ravel family at all.
const DASHBOARD_FILE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../deploy/grafana/dashboards/ravel.json"
);

/// The dashboard's shape, counted off the parsed JSON, and its distinct
/// `ravel_*` metric names.
///
/// Pinned as literals for the same reason [`EXPECTED_METRIC_NAMES`] is: a
/// walk that finds no panel (a renamed key, a path that stopped resolving, a
/// nesting shape the walk does not descend into) would otherwise leave the
/// per-name assertion below iterating an empty set and passing while checking
/// nothing. `deploy/README.md` states the row and name figures.
const EXPECTED_DASHBOARD_ROWS: usize = 6;
const EXPECTED_DASHBOARD_PANELS: usize = 36;
const EXPECTED_DASHBOARD_TARGETS: usize = 104;
const EXPECTED_DASHBOARD_METRIC_NAMES: usize = 107;

/// Families the dashboard graphs that no shipped rule alerts on.
///
/// Expected, and the reason the dashboard is not just a picture of the rule
/// file: most of what an operator reads during an incident (cache hit ratios,
/// planner estimates against actuals, store bytes by operation) is context no
/// threshold should page on. The complement is asserted in the other
/// direction: every name the rule file alerts on is graphed somewhere here,
/// so a page always has a panel to land on.
const EXPECTED_DASHBOARD_ONLY_NAMES: usize = 70;

/// Fenced `yaml` blocks in the guide, and the alert rules they hold between
/// them. Pinned so that an extractor that matches no block, or a block that
/// stops holding rules, fails here rather than leaving the per-alert
/// comparison below iterating an empty set.
const EXPECTED_GUIDE_BLOCKS: usize = 5;
const EXPECTED_GUIDE_ALERTS: usize = 13;

/// One tenant, one trivially valid PromQL rule. Enough for `alerting::spawn`
/// to build an evaluator, which is what puts the whole `ravel_alert_*` family
/// on the exposition: a process that configured no alerting omits the family
/// by design, so a default test server cannot prove those names render.
const ALERT_RULES_JSON: &str = r#"{
  "rules": [
    {
      "tenant": "acme",
      "rule_id": "shipped-rules-probe",
      "promql": "up",
      "condition": { "type": "threshold", "op": "gt", "value": 0.9 }
    }
  ]
}"#;

// ---------------------------------------------------------------------------
// A reader for the block-mapping subset a Prometheus rule file is written in.
// ---------------------------------------------------------------------------

/// A parsed node. The subset has no flow collections, no anchors, no tags and
/// no multi-document streams, so these three cases cover it.
#[derive(Debug)]
enum Value {
    Scalar(String),
    Mapping(Vec<(String, Value)>),
    Sequence(Vec<Value>),
}

/// One `- alert:` rule, with the fields the shipped file and the guide both
/// write.
#[derive(Debug)]
struct AlertRule {
    name: String,
    expr: String,
    /// The `for:` duration, absent on a rule transcribed from a source that
    /// states none.
    fires_after: Option<String>,
    labels: BTreeMap<String, String>,
    annotations: BTreeMap<String, String>,
}

#[derive(Debug)]
struct RuleGroup {
    name: String,
    rules: Vec<AlertRule>,
}

fn indent_of(line: &str) -> usize {
    line.len() - line.trim_start_matches(' ').len()
}

/// Splits `alert: RavelFoo` into its key and the text after the colon.
///
/// Returns `None` for anything that is not a mapping key, which is what turns
/// a spliced line into a parse failure rather than into text the reader walks
/// past.
fn split_key(content: &str) -> Option<(String, String)> {
    let colon = content.find(':')?;
    let key = &content[..colon];
    if key.is_empty()
        || !key
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
    {
        return None;
    }
    let after = &content[colon + 1..];
    if !after.is_empty() && !after.starts_with(' ') {
        return None;
    }
    Some((key.to_string(), after.trim().to_string()))
}

/// The value of a plain (unquoted, single-line) scalar.
///
/// A `#` preceded by a space opens a comment, which is why the `runbook:`
/// values keep their `#section` anchors: nothing there has a space before the
/// `#`.
fn plain_scalar(text: &str) -> Result<String, String> {
    let value = match text.find(" #") {
        Some(cut) => text[..cut].trim_end(),
        None => text,
    };
    let first = value
        .chars()
        .next()
        .ok_or_else(|| "a key was given an empty value".to_string())?;
    if matches!(first, '[' | '{' | '&' | '*' | '!' | '%' | '@' | '`') {
        return Err(format!(
            "{value:?} opens a YAML construct this reader does not accept"
        ));
    }
    if first == '"' || first == '\'' {
        if value.len() < 2 || !value.ends_with(first) {
            return Err(format!("{value:?} opens a quote that is never closed"));
        }
        return Ok(value[1..value.len() - 1].to_string());
    }
    Ok(value.to_string())
}

struct Reader {
    lines: Vec<String>,
    at: usize,
}

impl Reader {
    fn new(text: &str) -> Self {
        Reader {
            lines: text.lines().map(str::to_string).collect(),
            at: 0,
        }
    }

    fn line_no(&self) -> usize {
        self.at + 1
    }

    fn skip_ignorable(&mut self) {
        while let Some(line) = self.lines.get(self.at) {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                self.at += 1;
            } else {
                break;
            }
        }
    }

    /// Indentation and content of the next significant line, left in place.
    fn peek(&mut self) -> Option<(usize, String)> {
        self.skip_ignorable();
        let line = self.lines.get(self.at)?;
        let indent = indent_of(line);
        Some((indent, line[indent..].trim_end().to_string()))
    }

    /// Reads the mapping whose keys sit at `indent`, stopping at the first
    /// significant line that is shallower or is a sequence item.
    fn parse_mapping(&mut self, indent: usize) -> Result<Vec<(String, Value)>, String> {
        let mut entries: Vec<(String, Value)> = Vec::new();
        while let Some((line_indent, content)) = self.peek() {
            if line_indent < indent {
                break;
            }
            if line_indent > indent {
                return Err(format!(
                    "line {}: indented {line_indent} where a key at column {indent} was expected",
                    self.line_no()
                ));
            }
            if content.starts_with("- ") {
                break;
            }
            let key_line = self.line_no();
            let (key, after) = split_key(&content).ok_or_else(|| {
                format!("line {key_line}: this is not a mapping key: {content:?}")
            })?;
            if entries.iter().any(|(seen, _)| *seen == key) {
                return Err(format!("line {key_line}: duplicate key {key:?}"));
            }
            self.at += 1;
            let value = self.parse_value(indent, &after, key_line)?;
            entries.push((key, value));
        }
        Ok(entries)
    }

    /// Reads the sequence whose `- ` markers sit at `indent`. Every item in
    /// this subset is a mapping.
    fn parse_sequence(&mut self, indent: usize) -> Result<Vec<Value>, String> {
        let mut items = Vec::new();
        while let Some((line_indent, content)) = self.peek() {
            if line_indent < indent {
                break;
            }
            if line_indent > indent {
                return Err(format!(
                    "line {}: indented {line_indent} where a sequence item at column {indent} \
                     was expected",
                    self.line_no()
                ));
            }
            if !content.starts_with("- ") {
                return Err(format!(
                    "line {}: this is not a sequence item: {content:?}",
                    self.line_no()
                ));
            }
            self.lines[self.at].replace_range(indent..indent + 2, "  ");
            items.push(Value::Mapping(self.parse_mapping(indent + 2)?));
        }
        Ok(items)
    }

    fn parse_value(
        &mut self,
        key_indent: usize,
        after: &str,
        key_line: usize,
    ) -> Result<Value, String> {
        if after.is_empty() {
            let child = self
                .peek()
                .filter(|(child_indent, _)| *child_indent > key_indent);
            let Some((child_indent, content)) = child else {
                return Err(format!("line {key_line}: this key opens nothing below it"));
            };
            return if content.starts_with("- ") {
                Ok(Value::Sequence(self.parse_sequence(child_indent)?))
            } else {
                Ok(Value::Mapping(self.parse_mapping(child_indent)?))
            };
        }
        if matches!(after, "|" | "|-" | ">" | ">-") {
            return Ok(Value::Scalar(self.parse_block_scalar(key_indent, after)?));
        }
        Ok(Value::Scalar(
            plain_scalar(after).map_err(|e| format!("line {key_line}: {e}"))?,
        ))
    }

    /// Reads the block scalar a `|`, `|-`, `>` or `>-` header opened: every
    /// following line indented past the key, dedented by the least of their
    /// indents.
    ///
    /// A literal block joins those lines with newlines. A folded one joins
    /// them with spaces, and refuses a blank or a more-indented line, since
    /// those fold by rules this reader does not implement and must not be
    /// guessed at.
    fn parse_block_scalar(&mut self, key_indent: usize, style: &str) -> Result<String, String> {
        let opened_at = self.line_no();
        let mut raw: Vec<String> = Vec::new();
        while let Some(line) = self.lines.get(self.at) {
            if line.trim().is_empty() {
                raw.push(String::new());
                self.at += 1;
                continue;
            }
            if indent_of(line) <= key_indent {
                break;
            }
            raw.push(line.trim_end().to_string());
            self.at += 1;
        }
        while raw.last().is_some_and(String::is_empty) {
            raw.pop();
            self.at -= 1;
        }
        if raw.is_empty() {
            return Err(format!(
                "line {opened_at}: this block scalar has no content"
            ));
        }
        let base = raw
            .iter()
            .filter(|line| !line.is_empty())
            .map(|line| indent_of(line))
            .min()
            .unwrap_or(0);
        let body: Vec<String> = raw
            .iter()
            .map(|line| {
                if line.is_empty() {
                    String::new()
                } else {
                    line[base..].to_string()
                }
            })
            .collect();
        if style.starts_with('>') {
            if body
                .iter()
                .any(|line| line.is_empty() || indent_of(line) > 0)
            {
                return Err(format!(
                    "line {opened_at}: this folded block scalar has a blank or a more-indented \
                     line, which this reader does not fold"
                ));
            }
            return Ok(body.join(" "));
        }
        Ok(body.join("\n"))
    }
}

fn scalar(value: &Value, key: &str) -> Result<String, String> {
    match value {
        Value::Scalar(text) => Ok(text.clone()),
        _ => Err(format!("`{key}` must hold a scalar")),
    }
}

fn scalar_map(value: &Value, key: &str) -> Result<BTreeMap<String, String>, String> {
    let Value::Mapping(entries) = value else {
        return Err(format!("`{key}` must hold a mapping"));
    };
    entries
        .iter()
        .map(|(name, held)| Ok((name.clone(), scalar(held, name)?)))
        .collect()
}

fn build_rule(fields: &[(String, Value)]) -> Result<AlertRule, String> {
    let mut name = None;
    let mut expr = None;
    let mut fires_after = None;
    let mut labels = BTreeMap::new();
    let mut annotations = BTreeMap::new();
    for (key, value) in fields {
        match key.as_str() {
            "alert" => name = Some(scalar(value, "alert")?),
            "expr" => expr = Some(scalar(value, "expr")?),
            "for" => fires_after = Some(scalar(value, "for")?),
            "labels" => labels = scalar_map(value, "labels")?,
            "annotations" => annotations = scalar_map(value, "annotations")?,
            other => return Err(format!("unexpected key {other:?} in an alert rule")),
        }
    }
    Ok(AlertRule {
        name: name.ok_or_else(|| "a rule carries no `alert` key".to_string())?,
        expr: expr.ok_or_else(|| "a rule carries no `expr` key".to_string())?,
        fires_after,
        labels,
        annotations,
    })
}

/// Parses a Prometheus rule file, or one fenced block holding the same shape,
/// into its groups and rules.
///
/// Every non-blank, non-comment line has to be a mapping key, a sequence item
/// or a line of a block scalar some key opened, at an indentation its parent
/// allows, and every key has to be one this shape defines. A line that is
/// none of those is an error rather than text the reader steps over.
///
/// This is not a general YAML implementation and does not claim to accept
/// exactly what Prometheus accepts: it takes the subset these two files are
/// written in and refuses the rest, which is the direction that makes a
/// corrupt file fail rather than pass.
fn parse_rule_file(text: &str) -> Result<Vec<RuleGroup>, String> {
    if text.contains('\t') {
        return Err("a tab appears in the text; YAML forbids tabs in indentation".to_string());
    }
    let mut reader = Reader::new(text);
    let root = reader.parse_mapping(0)?;
    if let Some((_, content)) = reader.peek() {
        return Err(format!(
            "line {}: content past the end of the document: {content:?}",
            reader.line_no()
        ));
    }
    let top: Vec<&str> = root.iter().map(|(key, _)| key.as_str()).collect();
    if top != ["groups"] {
        return Err(format!(
            "the top level must be a single `groups` key, found {top:?}"
        ));
    }
    let Value::Sequence(groups) = &root[0].1 else {
        return Err("`groups` must hold a sequence".to_string());
    };

    let mut out = Vec::new();
    for group in groups {
        let Value::Mapping(fields) = group else {
            return Err("every entry of `groups` must be a mapping".to_string());
        };
        let mut name = None;
        let mut rules = None;
        for (key, value) in fields {
            match key.as_str() {
                "name" => name = Some(scalar(value, "name")?),
                "rules" => {
                    let Value::Sequence(items) = value else {
                        return Err("`rules` must hold a sequence".to_string());
                    };
                    let mut parsed = Vec::new();
                    for item in items {
                        let Value::Mapping(rule_fields) = item else {
                            return Err("every entry of `rules` must be a mapping".to_string());
                        };
                        parsed.push(build_rule(rule_fields)?);
                    }
                    rules = Some(parsed);
                }
                other => return Err(format!("unexpected key {other:?} in a rule group")),
            }
        }
        out.push(RuleGroup {
            name: name.ok_or_else(|| "a group carries no `name` key".to_string())?,
            rules: rules.ok_or_else(|| "a group carries no `rules` key".to_string())?,
        });
    }
    Ok(out)
}

/// The body of every ```` ```yaml ```` fenced block in a Markdown document.
fn fenced_yaml_blocks(markdown: &str) -> Vec<String> {
    let mut blocks = Vec::new();
    let mut current: Option<Vec<&str>> = None;
    for line in markdown.lines() {
        match current {
            None => {
                if line.trim_end() == "```yaml" {
                    current = Some(Vec::new());
                }
            }
            Some(ref mut body) => {
                if line.trim_end() == "```" {
                    blocks.push(body.join("\n"));
                    current = None;
                } else {
                    body.push(line);
                }
            }
        }
    }
    blocks
}

fn shipped_rule_groups() -> Vec<RuleGroup> {
    let text = std::fs::read_to_string(RULES_FILE)
        .unwrap_or_else(|e| panic!("shipped rule file {RULES_FILE} must be readable: {e}"));
    let groups = parse_rule_file(&text)
        .unwrap_or_else(|e| panic!("shipped rule file {RULES_FILE} must parse: {e}"));
    assert_eq!(
        groups.len(),
        EXPECTED_GROUPS,
        "shipped rule file must carry exactly {EXPECTED_GROUPS} groups, found {}: {:?}",
        groups.len(),
        groups.iter().map(|group| &group.name).collect::<Vec<_>>()
    );
    groups
}

/// Every `ravel_<segment>[_<segment>...]` token in `text`.
///
/// Hand-rolled rather than a regex so this test adds no dependency. A trailing
/// underscore is trimmed, so a family prefix written in prose (`ravel_scrub_`)
/// does not enter the set as a metric name, and a match that continues an
/// identifier is skipped.
fn metric_names(text: &str) -> BTreeSet<String> {
    const PREFIX: &str = "ravel_";
    let bytes = text.as_bytes();
    let mut out = BTreeSet::new();
    let mut cursor = 0usize;
    while let Some(offset) = text[cursor..].find(PREFIX) {
        let start = cursor + offset;
        let mut end = start + PREFIX.len();
        while end < bytes.len()
            && (bytes[end].is_ascii_lowercase()
                || bytes[end].is_ascii_digit()
                || bytes[end] == b'_')
        {
            end += 1;
        }
        let continues_identifier =
            start > 0 && (bytes[start - 1].is_ascii_alphanumeric() || bytes[start - 1] == b'_');
        let name = text[start..end].trim_end_matches('_');
        if !continues_identifier && name.len() > PREFIX.len() {
            out.insert(name.to_string());
        }
        cursor = end;
    }
    out
}

/// The families a rendered exposition really exposes, read off its `# TYPE`
/// lines, each mapped to the type that line declares.
///
/// Not [`metric_names`] over the whole body: a family's HELP text names other
/// families (`ravel_admission_admitted_bytes_total`'s names
/// `ravel_ingest_wire_bytes_total`), so a substring scan would accept a rule
/// whose metric only ever appears inside someone else's help string.
///
/// The type is carried because a histogram's `# TYPE` line names the base
/// family alone while its samples are `_bucket`, `_sum` and `_count`; see
/// [`is_rendered`].
fn exposed_families(body: &str) -> BTreeMap<String, String> {
    body.lines()
        .filter_map(|line| line.strip_prefix("# TYPE "))
        .filter_map(|rest| {
            let mut fields = rest.split_whitespace();
            Some((fields.next()?.to_string(), fields.next()?.to_string()))
        })
        .collect()
}

/// Whether `name` selects a series the exposition really carries.
///
/// A histogram or summary declares one `# TYPE` line for the base family and
/// renders `<family>_bucket`, `<family>_sum` and `<family>_count` samples
/// under it, so a `histogram_quantile` over `..._bucket` names a real series
/// that no `# TYPE` line spells. The suffix is only accepted when the base
/// family is really one of those two types: `ravel_store_bytes_total_count`
/// stays a miss.
fn is_rendered(name: &str, rendered: &BTreeMap<String, String>) -> bool {
    if rendered.contains_key(name) {
        return true;
    }
    ["_bucket", "_sum", "_count"].iter().any(|suffix| {
        name.strip_suffix(suffix)
            .and_then(|base| rendered.get(base))
            .is_some_and(|kind| kind == "histogram" || kind == "summary")
    })
}

// ---------------------------------------------------------------------------
// Reading metric names out of the shipped Grafana dashboard.
// ---------------------------------------------------------------------------

/// Blanks every quoted string in a PromQL expression, keeping its length.
///
/// A panel target is an EXPRESSION, not a bare metric name, and a label value
/// is arbitrary text: `{job="ravel_server"}` names no metric, but a scan of
/// the raw expression would put `ravel_server` in the set and then assert it
/// against `/metrics`, where it fails for a dashboard that is perfectly
/// correct. Blanking rather than deleting keeps the `:` check below honest
/// about where in the expression a colon sits.
///
/// PromQL's three string forms are handled: `"..."` and `'...'` with
/// backslash escapes, and raw backtick strings, which have none.
fn blank_strings(expr: &str) -> String {
    let mut out = String::with_capacity(expr.len());
    let mut chars = expr.chars();
    while let Some(c) = chars.next() {
        if c != '"' && c != '\'' && c != '`' {
            out.push(c);
            continue;
        }
        out.push(' ');
        let escapes = c != '`';
        while let Some(inner) = chars.next() {
            out.push(' ');
            if escapes && inner == '\\' {
                if chars.next().is_some() {
                    out.push(' ');
                }
                continue;
            }
            if inner == c {
                break;
            }
        }
    }
    out
}

/// Blanks every Grafana template variable (`$name`, `${name}`), keeping the
/// expression's length.
///
/// `$__rate_interval` is on nearly every target here, and `${ds}` selects the
/// datasource. None of them can name a metric family, so removing them before
/// the scan is what stops a variable that happened to be called `$ravel_x`
/// from entering the set: the `$` is not an identifier character, so the
/// scanner would otherwise read the name straight through it.
fn blank_template_variables(expr: &str) -> String {
    let bytes = expr.as_bytes();
    let mut out = String::with_capacity(expr.len());
    let mut at = 0usize;
    while at < bytes.len() {
        if bytes[at] != b'$' {
            out.push(expr[at..].chars().next().expect("char boundary"));
            at += expr[at..].chars().next().expect("char boundary").len_utf8();
            continue;
        }
        let mut end = at + 1;
        let braced = end < bytes.len() && bytes[end] == b'{';
        if braced {
            end += 1;
        }
        while end < bytes.len() && (bytes[end].is_ascii_alphanumeric() || bytes[end] == b'_') {
            end += 1;
        }
        if braced && end < bytes.len() && bytes[end] == b'}' {
            end += 1;
        }
        out.push_str(&" ".repeat(end - at));
        at = end;
    }
    out
}

/// The Ravel metric names one panel target's PromQL expression selects.
///
/// Strings and template variables are blanked first, then [`metric_names`]
/// runs over what is left, so the only tokens that can enter the set are
/// identifiers in the expression itself. PromQL function names cannot reach
/// it either: every one of them (`rate`, `sum`, `histogram_quantile`,
/// `clamp_min`, `time`) fails the `ravel_` prefix.
///
/// A colon in what is left is refused rather than scanned. A colon there can
/// only be a recording rule name (`job:ravel_x:rate5m`), whose left and right
/// segments would each be read as a metric, and whose real name never appears
/// on a `/metrics` body at all because Prometheus, not Ravel, computes it.
/// This dashboard uses no recording rules; adding one means deciding what
/// this test should hold it to, not silently widening the scan.
fn target_metric_names(expr: &str) -> BTreeSet<String> {
    let scannable = blank_template_variables(&blank_strings(expr));
    assert!(
        !scannable.contains(':'),
        "dashboard target {expr:?} holds a colon outside a string, which can only be a recording \
         rule name; no recording rule name appears on a /metrics body"
    );
    metric_names(&scannable)
}

/// What one walk of the dashboard found.
struct Dashboard {
    rows: usize,
    /// Objects carrying a `targets` array, wherever they sit.
    panels: usize,
    targets: usize,
    names: BTreeSet<String>,
}

/// Collects every object in the dashboard JSON that carries a `targets`
/// array, at any depth.
///
/// Grafana stores panels in two shapes and this dashboard emits both: an
/// uncollapsed row is a marker panel in the top-level `panels` array with its
/// content following it as siblings, while a collapsed row (the `Alerting`
/// row here) holds its content nested in its own `panels` array. A walk that
/// only read the top level would silently drop the whole collapsed row, which
/// is the failure the pinned counts exist to catch. Recursing over every
/// value covers both shapes and any third one a future Grafana export uses.
fn collect_target_holders<'a>(node: &'a serde_json::Value, out: &mut Vec<&'a serde_json::Value>) {
    match node {
        serde_json::Value::Object(fields) => {
            if fields
                .get("targets")
                .is_some_and(serde_json::Value::is_array)
            {
                out.push(node);
            }
            for value in fields.values() {
                collect_target_holders(value, out);
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                collect_target_holders(item, out);
            }
        }
        _ => {}
    }
}

/// Parses the shipped dashboard and reads every metric name its panel targets
/// select.
///
/// The three shapes the walk can meet, and what each does:
///
/// * A panel with no `targets` key at all (every row marker here, and any
///   text panel a later edit adds) queries nothing, so it contributes no name
///   and is not counted in [`EXPECTED_DASHBOARD_PANELS`]. An EMPTY `targets`
///   array is a different thing and is refused: that is a graph panel that
///   draws nothing, not a panel that was never meant to query.
/// * A target with no `expr` key, or one holding only whitespace, is refused.
///   Grafana saves such a target when a query was started and abandoned; it
///   draws nothing, and accepting it would let a panel quietly stop covering
///   what its title says it covers.
/// * A template variable inside an expression is blanked before the scan (see
///   [`blank_template_variables`]). A `templating.list` entry of type `query`
///   is refused outright: its own query is PromQL that this walk does not
///   read, so allowing one would put a metric reference in the dashboard that
///   nothing here holds to the rendered body.
fn shipped_dashboard() -> Dashboard {
    let text = std::fs::read_to_string(DASHBOARD_FILE)
        .unwrap_or_else(|e| panic!("shipped dashboard {DASHBOARD_FILE} must be readable: {e}"));
    let dashboard: serde_json::Value = serde_json::from_str(&text)
        .unwrap_or_else(|e| panic!("shipped dashboard {DASHBOARD_FILE} must be valid JSON: {e}"));

    for variable in dashboard["templating"]["list"]
        .as_array()
        .unwrap_or_else(|| panic!("{DASHBOARD_FILE} must carry a templating.list array"))
    {
        let kind = variable["type"].as_str().unwrap_or_else(|| {
            panic!("every templating entry of {DASHBOARD_FILE} must declare a type")
        });
        assert_ne!(
            kind, "query",
            "templating variable {:?} is a query variable, whose own PromQL this test does not \
             read; a metric named only there would never be held to a rendered body",
            variable["name"]
        );
    }

    let mut holders = Vec::new();
    collect_target_holders(&dashboard, &mut holders);

    let mut targets = 0usize;
    let mut names = BTreeSet::new();
    for panel in &holders {
        let title = panel["title"].as_str().unwrap_or("<untitled>");
        let panel_targets = panel["targets"].as_array().expect("checked above");
        assert!(
            !panel_targets.is_empty(),
            "panel {title:?} of {DASHBOARD_FILE} carries an empty `targets` array, so it draws \
             nothing; a panel that is not meant to query carries no `targets` key"
        );
        for target in panel_targets {
            targets += 1;
            let expr = target["expr"].as_str().unwrap_or_else(|| {
                panic!("a target of panel {title:?} of {DASHBOARD_FILE} carries no `expr` string")
            });
            assert!(
                !expr.trim().is_empty(),
                "a target of panel {title:?} of {DASHBOARD_FILE} carries an empty `expr`, so it \
                 draws nothing"
            );
            names.extend(target_metric_names(expr));
        }
    }

    let rows = dashboard["panels"]
        .as_array()
        .unwrap_or_else(|| panic!("{DASHBOARD_FILE} must carry a panels array"))
        .iter()
        .filter(|panel| panel["type"] == "row")
        .count();

    Dashboard {
        rows,
        panels: holders.len(),
        targets,
        names,
    }
}

/// Mirrors `metrics_endpoint.rs`'s helper, with the three knobs this test
/// needs: the fold task (the stamp-coverage families), a deployment key (the
/// durable auth family) and an alert rule set (the alerting family).
async fn start_test_server(
    mode: Mode,
    fold_enabled: bool,
    deployment_key: bool,
    alert_rules: bool,
) -> ravel_server::Running {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new("acme"));
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    let store = Arc::new(MemoryStore::new());
    let alerting = if alert_rules {
        ravel_server::AlertEvalConfig {
            enabled: true,
            rules: Arc::new(
                ravel_server::alerting::parse_rules(ALERT_RULES_JSON).expect("rules parse"),
            ),
            ..ravel_server::AlertEvalConfig::default()
        }
    } else {
        ravel_server::AlertEvalConfig::default()
    };
    let config = ServerConfig {
        audit_pipeline: Default::default(),
        audit_text: Default::default(),
        query_budgets: Default::default(),
        max_inflight_flushes: 1,
        adaptive_flush_delay: false,
        max_flush_delay: std::time::Duration::from_secs(2),
        max_flush_delay_idle: std::time::Duration::from_secs(40),
        min_flush_bytes: 256 * 1024,
        mode,
        listen_http: "127.0.0.1:0".parse().expect("valid loopback addr"),
        listen_grpc: "127.0.0.1:0".parse().expect("valid loopback addr"),
        shard_count: 1,
        tenant_resolver,
        mtls_listener: None,
        fold_tenants: Vec::new(),
        fold: FoldTaskConfig {
            enabled: fold_enabled,
            ..FoldTaskConfig::default()
        },
        maintain: ravel_server::MaintenanceTaskConfig::default(),
        alerting,
        oidc_refresh: None,
        otap: false,
        metrics_tenant_labels: false,
        limits: ravel_server::LimitsConfig::default(),
        max_ingest_lag: ravel_server::DEFAULT_MAX_INGEST_LAG,
        deployment_key: deployment_key.then(|| Box::new([7u8; 32])),
        gc: ravel_maintain::GcConfigValues::maintain_defaults(),
        query_deadline: ravel_query::EngineConfig::default().deadline,
        store_probe_interval: ravel_server::store_probe::DEFAULT_STORE_PROBE_INTERVAL,
        admission_reconcile_interval: ravel_ingest::DEFAULT_ADMISSION_RECONCILE_INTERVAL,
        query_concurrency_limit: ravel_query::QueryConcurrencyLimit::Unlimited,
        max_s3_requests: ravel_query::EngineConfig::default().max_s3_requests,
        scrub_period: std::time::Duration::from_secs(7 * 86_400),
        indexed_fields: Default::default(),
        typed_attr_columns: Default::default(),
        disable_cache: false,
        cache_max_bytes: 256 * 1024 * 1024,
        catalog_cache_max_bytes: 256 * 1024 * 1024,
        process_memory_budget_bytes: u64::MAX,
        process_memory_budget_is_fallback: false,
        cache_dir: None,
        catalog_resolve_concurrency: None,
        ingest_buffer_budget_limit: ravel_server::IngestByteBudgetLimit::Unlimited,
        idle_tenant_state_ttl: std::time::Duration::from_secs(3600),
        distrib: None,
        remote_clusters: Vec::new(),
        shutdown_timeout: ravel_server::DEFAULT_SHUTDOWN_TIMEOUT,
        drain_settle_interval: std::time::Duration::ZERO,
        ingest_concurrency_limit: ravel_server::ingest_concurrency::IngestConcurrencyLimit::Bounded(
            1024,
        ),
    };
    ravel_server::start(
        config,
        store.clone(),
        store.clone(),
        Arc::new(StoreMetrics::default()),
        None,
    )
    .await
    .expect("server starts")
}

async fn scrape(running: &ravel_server::Running) -> String {
    let base = format!("http://{}", running.http_addr);
    reqwest::Client::new()
        .get(format!("{base}/metrics"))
        .send()
        .await
        .expect("metrics request completes")
        .text()
        .await
        .expect("metrics body is text")
}

/// The union of the `/metrics` bodies of the two process shapes it takes to
/// render every family a shipped rule names.
///
/// No single process renders them all, and that is by design rather than an
/// accident of this test: the maintenance and scrub families exist only on a
/// `--mode maintain` process, while the ingest, fold stamp-coverage, durable
/// auth and alerting families exist only where those subsystems were built.
async fn rendered_bodies() -> String {
    let all = start_test_server(Mode::All, true, true, true).await;
    let mut body = scrape(&all).await;
    all.shutdown().await.expect("graceful shutdown");

    let maintain = start_test_server(Mode::Maintain, false, false, false).await;
    body.push_str(&scrape(&maintain).await);
    maintain.shutdown().await.expect("graceful shutdown");

    body
}

#[tokio::test]
async fn every_metric_named_by_a_shipped_rule_is_rendered() {
    let groups = shipped_rule_groups();
    let alerts: Vec<&AlertRule> = groups.iter().flat_map(|group| group.rules.iter()).collect();
    assert_eq!(
        alerts.len(),
        EXPECTED_ALERTS,
        "shipped rule file must carry exactly {EXPECTED_ALERTS} alert rules"
    );

    let undated: Vec<&str> = alerts
        .iter()
        .filter(|alert| alert.fires_after.is_none())
        .map(|alert| alert.name.as_str())
        .collect();
    assert_eq!(
        undated.len(),
        EXPECTED_ALERTS_WITHOUT_FOR,
        "exactly {EXPECTED_ALERTS_WITHOUT_FOR} rules come from a source that states no duration \
         and so carry no `for:`, found {undated:?}"
    );
    for alert in &alerts {
        let name = alert.name.as_str();
        assert!(
            alert.annotations.contains_key("runbook"),
            "alert {name:?} carries no `runbook` annotation naming the section to read"
        );
        assert!(
            alert.fires_after.is_some() || alert.annotations.contains_key("as_documented"),
            "alert {name:?} carries no `for:` and no `as_documented` annotation saying why"
        );
    }

    let mut names = BTreeSet::new();
    for alert in &alerts {
        names.extend(metric_names(&alert.expr));
        for text in alert.annotations.values() {
            names.extend(metric_names(text));
        }
    }
    assert_eq!(
        names.len(),
        EXPECTED_METRIC_NAMES,
        "shipped rule file must name exactly {EXPECTED_METRIC_NAMES} distinct Ravel metrics, \
         found {}: {names:?}",
        names.len()
    );

    let body = rendered_bodies().await;
    let rendered = exposed_families(&body);
    assert!(
        rendered.len() > EXPECTED_METRIC_NAMES,
        "the rendered bodies must expose more metrics than the rule file names; \
         found only {}",
        rendered.len()
    );

    let missing: Vec<&String> = names
        .iter()
        .filter(|n| !is_rendered(n, &rendered))
        .collect();
    assert!(
        missing.is_empty(),
        "shipped rules name metrics no /metrics body renders: {missing:?}"
    );
}

/// The guide's fenced blocks are what a reader copies off the page. Where the
/// guide and the shipped file both carry a rule, the condition has to be the
/// same one, or the page hands out a rule that fires differently from the one
/// the operator was told to load.
#[test]
fn the_guide_and_the_shipped_rule_file_agree() {
    let groups = shipped_rule_groups();
    let shipped: BTreeMap<&str, &AlertRule> = groups
        .iter()
        .flat_map(|group| group.rules.iter())
        .map(|rule| (rule.name.as_str(), rule))
        .collect();
    assert_eq!(
        shipped.len(),
        EXPECTED_ALERTS,
        "shipped rule file must carry exactly {EXPECTED_ALERTS} distinctly named alert rules"
    );

    let guide = std::fs::read_to_string(GUIDE_FILE)
        .unwrap_or_else(|e| panic!("guide {GUIDE_FILE} must be readable: {e}"));
    let blocks = fenced_yaml_blocks(&guide);
    assert_eq!(
        blocks.len(),
        EXPECTED_GUIDE_BLOCKS,
        "guide must hold exactly {EXPECTED_GUIDE_BLOCKS} fenced yaml blocks, found {}",
        blocks.len()
    );

    let mut guide_rules: Vec<AlertRule> = Vec::new();
    for (index, block) in blocks.iter().enumerate() {
        let parsed = parse_rule_file(block).unwrap_or_else(|e| {
            panic!(
                "fenced yaml block {} of {GUIDE_FILE} must parse: {e}",
                index + 1
            )
        });
        guide_rules.extend(parsed.into_iter().flat_map(|group| group.rules));
    }
    assert_eq!(
        guide_rules.len(),
        EXPECTED_GUIDE_ALERTS,
        "guide's fenced blocks must hold exactly {EXPECTED_GUIDE_ALERTS} alert rules, found {}",
        guide_rules.len()
    );

    for rule in &guide_rules {
        let name = rule.name.as_str();
        let Some(shipped_rule) = shipped.get(name) else {
            panic!(
                "the guide prints alert {name:?}, which the shipped rule file does not carry; \
                 a rule a reader can copy off the page must be one the shipped file ships"
            );
        };
        assert_eq!(
            shipped_rule.expr, rule.expr,
            "alert {name:?}: the guide's expression is not the shipped one"
        );
        assert_eq!(
            shipped_rule.fires_after, rule.fires_after,
            "alert {name:?}: the guide's `for:` duration is not the shipped one"
        );
        let shipped_severity = shipped_rule.labels.get("severity");
        assert_eq!(
            shipped_severity,
            rule.labels.get("severity"),
            "alert {name:?}: the guide's severity label is not the shipped one"
        );
        assert!(
            shipped_severity.is_some(),
            "alert {name:?} carries no severity label"
        );
    }
}

/// The shipped dashboard is held to the same standard as the shipped rule
/// file: a panel whose target selects a family Ravel does not render draws a
/// flat empty graph, and an operator reading it during an incident concludes
/// the subsystem is idle rather than that the panel is broken.
///
/// The assertion is against the RENDERED exposition, not against the string
/// literals in `services/ravel-server/src/metrics.rs`: a family declared in
/// the source that nothing ever writes a sample for, or one behind an
/// off-by-default feature, passes a source scan and still leaves the panel
/// empty.
#[tokio::test]
async fn every_metric_named_by_the_dashboard_is_rendered() {
    let dashboard = shipped_dashboard();
    assert_eq!(
        dashboard.rows, EXPECTED_DASHBOARD_ROWS,
        "the dashboard must carry exactly {EXPECTED_DASHBOARD_ROWS} rows, found {}",
        dashboard.rows
    );
    assert_eq!(
        dashboard.panels, EXPECTED_DASHBOARD_PANELS,
        "the dashboard must carry exactly {EXPECTED_DASHBOARD_PANELS} querying panels, found {}",
        dashboard.panels
    );
    assert_eq!(
        dashboard.targets, EXPECTED_DASHBOARD_TARGETS,
        "the dashboard must carry exactly {EXPECTED_DASHBOARD_TARGETS} panel targets, found {}",
        dashboard.targets
    );
    assert_eq!(
        dashboard.names.len(),
        EXPECTED_DASHBOARD_METRIC_NAMES,
        "the dashboard must name exactly {EXPECTED_DASHBOARD_METRIC_NAMES} distinct Ravel \
         metrics, found {}: {:?}",
        dashboard.names.len(),
        dashboard.names
    );

    let body = rendered_bodies().await;
    let rendered = exposed_families(&body);
    let missing: Vec<&String> = dashboard
        .names
        .iter()
        .filter(|name| !is_rendered(name, &rendered))
        .collect();
    assert!(
        missing.is_empty(),
        "the dashboard graphs metrics no /metrics body renders: {missing:?}"
    );
}

/// What the dashboard covers beyond the rule file, and what it must not
/// leave uncovered.
///
/// Every metric a shipped rule alerts on has to be graphed here, so a page
/// has a panel to land on. The converse is not required and is not true: most
/// of the dashboard is context no threshold should page on.
#[test]
fn the_dashboard_graphs_every_metric_a_shipped_rule_alerts_on() {
    let groups = shipped_rule_groups();
    let mut alerted = BTreeSet::new();
    for alert in groups.iter().flat_map(|group| group.rules.iter()) {
        alerted.extend(metric_names(&alert.expr));
        for text in alert.annotations.values() {
            alerted.extend(metric_names(text));
        }
    }
    assert_eq!(
        alerted.len(),
        EXPECTED_METRIC_NAMES,
        "shipped rule file must name exactly {EXPECTED_METRIC_NAMES} distinct Ravel metrics"
    );

    let dashboard = shipped_dashboard();
    let ungraphed: Vec<&String> = alerted
        .iter()
        .filter(|name| !dashboard.names.contains(*name))
        .collect();
    assert!(
        ungraphed.is_empty(),
        "these metrics carry a shipped alert and no dashboard panel: {ungraphed:?}"
    );

    let dashboard_only = dashboard.names.difference(&alerted).count();
    assert_eq!(
        dashboard_only, EXPECTED_DASHBOARD_ONLY_NAMES,
        "the dashboard must graph exactly {EXPECTED_DASHBOARD_ONLY_NAMES} metrics no shipped rule \
         alerts on, found {dashboard_only}"
    );
}

/// The extractor reads metric names out of an EXPRESSION, and the two things
/// in a PromQL expression that look like identifiers and are not are a label
/// value and a template variable.
///
/// Pinned here rather than left to the dashboard, which today contains no
/// label value holding the `ravel_` substring: the guard is what keeps a
/// later `{job="ravel_server"}` filter from putting a name that is not a
/// metric into the set and failing a dashboard that is correct.
#[test]
fn the_extractor_reads_selectors_and_not_label_values_or_variables() {
    let names = target_metric_names(
        "sum by (op) (rate(ravel_store_calls_total{job=\"ravel_server\", pod=~\"$ravel_pod\"}\
         [$__rate_interval])) / clamp_min(ravel_store_ok_total, 1)",
    );
    assert_eq!(
        names,
        BTreeSet::from([
            "ravel_store_calls_total".to_string(),
            "ravel_store_ok_total".to_string(),
        ]),
        "only the two selectors are metric names; the job label value and the pod variable are not"
    );
}
