//! Renders a syntax tree back to ES|QL text.

use std::fmt::Write as _;

use super::{
    ArrayLiteral, Binary, BinaryOp, Call, CallArgs, Cast, ChangePointCommand, Command,
    EnrichCommand, Expr, Field, FromCommand, FromSource, FuseCommand, FuseMethod, InList,
    InSubquery, IsNull, Like, LikeKind, LiteralValue, MapLiteral, NamePart, NullsOrder, ParamName,
    PromqlCommand, PromqlValue, QualifiedName, Query, SortCommand, SortOrder, SourcePattern,
    StatsCommand, Subquery, Unary, UnaryOp, prec,
};
use crate::error::Error;
use crate::limits::{LimitKind, Limits};
use crate::span::Span;

/// Renders `query` as ES|QL text, adding parentheses only where precedence
/// requires them.
///
/// For a tree produced by [`crate::parse`], when the printed text is itself
/// within `limits`, parsing it with the same limits yields a tree equal to
/// `query` under [`Query::eq_ignoring_spans`]. The printer adds spaces around
/// operators and writes a tab, newline, carriage return, quote or backslash in
/// a string as a two-byte escape, so a statement at the byte limit can print
/// to text past it; parsing that text returns the
/// [`LimitKind::StatementBytes`] limit error. A tree built by hand need not
/// round-trip: a right-leaning chain such as `a - (b - (c - ...))` at the
/// nesting limit prints one pair of parentheses per level, and parsing that
/// text counts each pair as a level and returns the
/// [`LimitKind::NestingDepth`] limit error.
///
/// Refuses an expression or nested pipeline deeper than
/// [`Limits::max_depth`], because the printer recurses once per level.
pub fn print(query: &Query, limits: &Limits) -> Result<String, Error> {
    let mut printer = Printer {
        out: String::new(),
        depth: 0,
        limits,
    };
    for setting in &query.settings {
        printer.out.push_str("SET ");
        push_part(&mut printer.out, &setting.name);
        printer.out.push_str(" = ");
        printer.expr(&setting.value, 0)?;
        printer.out.push_str("; ");
    }
    printer.pipeline(query)?;
    Ok(printer.out)
}

struct Printer<'a> {
    out: String,
    depth: usize,
    limits: &'a Limits,
}

fn push_string(out: &mut String, value: &str) {
    out.push('"');
    for c in value.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c => out.push(c),
        }
    }
    out.push('"');
}

fn push_part(out: &mut String, part: &NamePart) {
    if let Some(param) = &part.param {
        push_param(out, &param.name, param.identifier);
    } else if part.quoted {
        out.push('`');
        out.push_str(&part.text.replace('`', "``"));
        out.push('`');
    } else {
        out.push_str(&part.text);
    }
}

fn push_name(out: &mut String, name: &QualifiedName) {
    for (index, part) in name.parts.iter().enumerate() {
        if index > 0 {
            out.push('.');
        }
        push_part(out, part);
    }
}

fn push_names(out: &mut String, names: &[QualifiedName]) {
    for (index, name) in names.iter().enumerate() {
        if index > 0 {
            out.push_str(", ");
        }
        push_name(out, name);
    }
}

fn push_param(out: &mut String, name: &ParamName, identifier: bool) {
    out.push_str(if identifier { "??" } else { "?" });
    match name {
        ParamName::Positional => {}
        ParamName::Numbered(digits) => out.push_str(digits),
        ParamName::Named(name) => out.push_str(name),
    }
}

fn push_source(out: &mut String, source: &SourcePattern) {
    if source.quoted {
        push_string(out, &source.text);
    } else {
        out.push_str(&source.text);
    }
}

fn push_from(out: &mut String, keyword: &str, c: &FromCommand) {
    out.push_str(keyword);
    for (index, source) in c.sources.iter().enumerate() {
        out.push_str(if index > 0 { ", " } else { " " });
        push_source(out, source);
    }
    if let Some(metadata) = &c.metadata {
        out.push_str(" METADATA ");
        push_names(out, &metadata.fields);
    }
}

fn push_promql(out: &mut String, c: &PromqlCommand) {
    out.push_str("PROMQL ");
    for param in &c.params {
        out.push_str(&param.name);
        out.push_str(" = ");
        match &param.value {
            PromqlValue::Text(text) => push_source(out, text),
            PromqlValue::Param(p) => push_param(out, &p.name, p.identifier),
        }
        out.push(' ');
    }
    out.push('(');
    out.push_str(&c.query.text);
    out.push(')');
}

fn push_sort(out: &mut String, key_order: Option<SortOrder>, nulls: Option<NullsOrder>) {
    match key_order {
        Some(SortOrder::Asc) => out.push_str(" ASC"),
        Some(SortOrder::Desc) => out.push_str(" DESC"),
        None => {}
    }
    match nulls {
        Some(NullsOrder::First) => out.push_str(" NULLS FIRST"),
        Some(NullsOrder::Last) => out.push_str(" NULLS LAST"),
        None => {}
    }
}

fn push_enrich_head(out: &mut String, c: &EnrichCommand) {
    out.push_str("ENRICH ");
    push_source(out, &c.policy);
    if let Some(on) = &c.on {
        out.push_str(" ON ");
        push_name(out, on);
    }
    for (index, field) in c.with.iter().enumerate() {
        out.push_str(if index > 0 { ", " } else { " WITH " });
        if let Some(name) = &field.name {
            push_name(out, name);
            out.push_str(" = ");
        }
        push_name(out, &field.field);
    }
}

fn push_change_point(out: &mut String, c: &ChangePointCommand) {
    out.push_str("CHANGE_POINT ");
    push_name(out, &c.value);
    if let Some(key) = &c.key {
        out.push_str(" ON ");
        push_name(out, key);
    }
    if let Some(output) = &c.output {
        out.push_str(" AS ");
        push_name(out, &output.type_name);
        out.push_str(", ");
        push_name(out, &output.pvalue_name);
    }
}

/// Whether `text` begins with the word `from`, in any case.
fn starts_with_from(text: &str) -> bool {
    let bytes = text.as_bytes();
    bytes
        .get(..4)
        .is_some_and(|word| word.eq_ignore_ascii_case(b"from"))
        && !bytes
            .get(4)
            .is_some_and(|&b| b.is_ascii_alphanumeric() || b == b'_' || b == b'@')
}

/// Whether an `IN` list element needs parentheses beyond its precedence: the
/// parser admits a predicate there only when parenthesised.
fn is_predicate(expr: &Expr) -> bool {
    match expr {
        Expr::In(_) | Expr::InSubquery(_) | Expr::Like(_) | Expr::IsNull(_) => true,
        Expr::Binary(binary) => binary.op == BinaryOp::Match,
        _ => false,
    }
}

impl Printer<'_> {
    fn enter(&mut self, span: Span) -> Result<(), Error> {
        self.depth += 1;
        if self.depth > self.limits.max_depth {
            return Err(Error::LimitExceeded {
                limit: LimitKind::NestingDepth,
                max: self.limits.max_depth,
                span,
            });
        }
        Ok(())
    }

    fn pipeline(&mut self, query: &Query) -> Result<(), Error> {
        self.command(&query.source)?;
        self.commands_after(&query.processing, " | ")
    }

    /// Prints each command, preceded by `first` for the first and by ` | `
    /// for the rest.
    fn commands_after(&mut self, commands: &[Command], first: &str) -> Result<(), Error> {
        for (index, command) in commands.iter().enumerate() {
            self.out.push_str(if index > 0 { " | " } else { first });
            self.command(command)?;
        }
        Ok(())
    }

    fn subquery(&mut self, subquery: &Subquery) -> Result<(), Error> {
        self.enter(subquery.span)?;
        self.out.push('(');
        self.pipeline(&subquery.query)?;
        self.out.push(')');
        self.depth -= 1;
        Ok(())
    }

    fn command(&mut self, command: &Command) -> Result<(), Error> {
        match command {
            Command::FromQueries(c) => {
                self.out.push_str("FROM");
                for (index, source) in c.sources.iter().enumerate() {
                    self.out.push_str(if index > 0 { ", " } else { " " });
                    match source {
                        FromSource::Pattern(pattern) => push_source(&mut self.out, pattern),
                        FromSource::Query(query) => self.subquery(query)?,
                    }
                }
                Ok(())
            }
            Command::Fork(c) => {
                self.out.push_str("FORK");
                for branch in &c.branches {
                    self.enter(branch.span)?;
                    self.commands_after(&branch.commands, " (")?;
                    self.out.push(')');
                    self.depth -= 1;
                }
                Ok(())
            }
            other => self.flat_command(other),
        }
    }

    /// Prints a command that holds no nested pipeline.
    #[inline(never)]
    fn flat_command(&mut self, command: &Command) -> Result<(), Error> {
        match command {
            Command::Row(c) => {
                self.out.push_str("ROW ");
                self.fields(&c.fields)?;
            }
            Command::From(c) => push_from(&mut self.out, "FROM", c),
            Command::Ts(c) => push_from(&mut self.out, "TS", c),
            Command::Show(_) => self.out.push_str("SHOW INFO"),
            Command::Promql(c) => push_promql(&mut self.out, c),
            Command::Where(c) => {
                self.out.push_str("WHERE ");
                self.expr(&c.condition, 0)?;
            }
            Command::Eval(c) => {
                self.out.push_str("EVAL ");
                self.fields(&c.fields)?;
            }
            Command::Keep(c) => {
                self.out.push_str("KEEP ");
                push_names(&mut self.out, &c.patterns);
            }
            Command::Drop(c) => {
                self.out.push_str("DROP ");
                push_names(&mut self.out, &c.patterns);
            }
            Command::Rename(c) => {
                self.out.push_str("RENAME ");
                for (index, pair) in c.renames.iter().enumerate() {
                    if index > 0 {
                        self.out.push_str(", ");
                    }
                    push_name(&mut self.out, &pair.old);
                    self.out.push_str(" AS ");
                    push_name(&mut self.out, &pair.new);
                }
            }
            Command::Stats(c) => self.stats("STATS", c)?,
            Command::InlineStats(c) => self.stats("INLINE STATS", c)?,
            Command::Sort(c) => self.sort(c)?,
            Command::Limit(c) => {
                let _ = write!(self.out, "LIMIT {}", c.count);
            }
            Command::LimitBy(c) => {
                let _ = write!(self.out, "LIMIT {} BY ", c.count);
                push_names(&mut self.out, &c.groupings);
            }
            Command::Dissect(c) => {
                self.out.push_str("DISSECT ");
                self.expr(&c.input, prec::CAST)?;
                self.out.push(' ');
                push_string(&mut self.out, &c.pattern);
                if let Some(separator) = &c.append_separator {
                    self.out.push_str(" APPEND_SEPARATOR = ");
                    push_string(&mut self.out, separator);
                }
            }
            Command::Grok(c) => {
                self.out.push_str("GROK ");
                self.expr(&c.input, prec::CAST)?;
                self.out.push(' ');
                push_string(&mut self.out, &c.pattern);
            }
            Command::MvExpand(c) => {
                self.out.push_str("MV_EXPAND ");
                push_name(&mut self.out, &c.field);
            }
            Command::LookupJoin(c) => {
                self.out.push_str("LOOKUP JOIN ");
                push_source(&mut self.out, &c.index);
                self.out.push_str(" ON ");
                push_names(&mut self.out, &c.on);
            }
            Command::Enrich(c) => push_enrich_head(&mut self.out, c),
            Command::Fuse(c) => self.fuse(c)?,
            Command::ChangePoint(c) => push_change_point(&mut self.out, c),
            Command::Sample(c) => {
                self.out.push_str("SAMPLE ");
                self.out.push_str(&c.probability);
            }
            Command::Completion(c) => {
                self.out.push_str("COMPLETION ");
                if let Some(target) = &c.target {
                    push_name(&mut self.out, target);
                    self.out.push_str(" = ");
                }
                self.expr(&c.prompt, prec::CAST)?;
                self.out.push_str(" WITH ");
                self.options(&c.options)?;
            }
            Command::Rerank(c) => {
                self.out.push_str("RERANK ");
                if let Some(target) = &c.target {
                    push_name(&mut self.out, target);
                    self.out.push_str(" = ");
                }
                self.expr(&c.query, 0)?;
                self.out.push_str(" ON ");
                push_names(&mut self.out, &c.on);
                self.out.push_str(" WITH ");
                self.options(&c.options)?;
            }
            Command::FromQueries(_) | Command::Fork(_) => return self.command(command),
        }
        Ok(())
    }

    fn stats(&mut self, keyword: &str, c: &StatsCommand) -> Result<(), Error> {
        self.out.push_str(keyword);
        for (index, aggregate) in c.aggregates.iter().enumerate() {
            self.out.push_str(if index > 0 { ", " } else { " " });
            self.field(&aggregate.field)?;
            if let Some(filter) = &aggregate.filter {
                self.out.push_str(" WHERE ");
                self.expr(filter, 0)?;
            }
        }
        if !c.groupings.is_empty() {
            self.out.push_str(" BY ");
            self.fields(&c.groupings)?;
        }
        Ok(())
    }

    fn sort(&mut self, c: &SortCommand) -> Result<(), Error> {
        self.out.push_str("SORT ");
        for (index, key) in c.keys.iter().enumerate() {
            if index > 0 {
                self.out.push_str(", ");
            }
            self.expr(&key.expr, 0)?;
            push_sort(&mut self.out, key.order, key.nulls);
        }
        Ok(())
    }

    fn fuse(&mut self, c: &FuseCommand) -> Result<(), Error> {
        self.out.push_str("FUSE");
        match c.method {
            Some(FuseMethod::Rrf) => self.out.push_str(" RRF"),
            Some(FuseMethod::Linear) => self.out.push_str(" LINEAR"),
            None => {}
        }
        if let Some(score_by) = &c.score_by {
            self.out.push_str(" SCORE BY ");
            push_name(&mut self.out, score_by);
        }
        if !c.key_by.is_empty() {
            self.out.push_str(" KEY BY ");
            push_names(&mut self.out, &c.key_by);
        }
        if let Some(group_by) = &c.group_by {
            self.out.push_str(" GROUP BY ");
            push_name(&mut self.out, group_by);
        }
        if let Some(options) = &c.options {
            self.out.push_str(" WITH ");
            self.options(options)?;
        }
        Ok(())
    }

    /// A command's `WITH` option map, one level deep.
    fn options(&mut self, map: &MapLiteral) -> Result<(), Error> {
        self.enter(map.span)?;
        self.map(map)?;
        self.depth -= 1;
        Ok(())
    }

    fn map(&mut self, map: &MapLiteral) -> Result<(), Error> {
        self.out.push('{');
        for (index, entry) in map.entries.iter().enumerate() {
            if index > 0 {
                self.out.push_str(", ");
            }
            push_string(&mut self.out, &entry.key);
            self.out.push_str(": ");
            self.expr(&entry.value, 0)?;
        }
        self.out.push('}');
        Ok(())
    }

    fn field(&mut self, field: &Field) -> Result<(), Error> {
        if let Some(name) = &field.name {
            push_name(&mut self.out, name);
            self.out.push_str(" = ");
        }
        self.expr(&field.expr, 0)
    }

    fn fields(&mut self, fields: &[Field]) -> Result<(), Error> {
        for (index, field) in fields.iter().enumerate() {
            if index > 0 {
                self.out.push_str(", ");
            }
            self.field(field)?;
        }
        Ok(())
    }

    fn exprs(&mut self, exprs: &[Expr], min_prec: u8) -> Result<(), Error> {
        for (index, expr) in exprs.iter().enumerate() {
            if index > 0 {
                self.out.push_str(", ");
            }
            self.expr(expr, min_prec)?;
        }
        Ok(())
    }

    /// Prints `expr`, parenthesised when its outermost operator binds more
    /// loosely than `min_prec`.
    fn expr(&mut self, expr: &Expr, min_prec: u8) -> Result<(), Error> {
        self.enter(expr.span())?;
        let parens = expr.precedence() < min_prec;
        if parens {
            self.out.push('(');
        }
        self.expr_body(expr)?;
        if parens {
            self.out.push(')');
        }
        self.depth -= 1;
        Ok(())
    }

    // Each nesting form prints in its own function and this one only
    // dispatches, so the frame repeated per nesting level stays small in an
    // unoptimised build.
    fn expr_body(&mut self, expr: &Expr) -> Result<(), Error> {
        match expr {
            Expr::Call(call) => self.call(call),
            Expr::Unary(unary) => self.unary(unary),
            Expr::Binary(binary) => self.binary(binary),
            Expr::In(list) => self.in_list(list),
            Expr::InSubquery(in_query) => self.in_subquery(in_query),
            Expr::Like(like) => self.like(like),
            Expr::IsNull(is_null) => self.is_null(is_null),
            Expr::Cast(cast) => self.cast(cast),
            Expr::Array(array) => self.array(array),
            Expr::Map(map) => self.map(map),
            leaf => {
                self.leaf(leaf);
                Ok(())
            }
        }
    }

    fn call(&mut self, call: &Call) -> Result<(), Error> {
        self.out.push_str(&call.name);
        self.out.push('(');
        match &call.args {
            CallArgs::Star => self.out.push('*'),
            CallArgs::List(args) => self.exprs(args, 0)?,
        }
        self.out.push(')');
        Ok(())
    }

    fn unary(&mut self, unary: &Unary) -> Result<(), Error> {
        let min = match unary.op {
            UnaryOp::Not => {
                self.out.push_str("NOT ");
                prec::NOT
            }
            UnaryOp::Minus => {
                self.out.push('-');
                prec::UNARY
            }
            UnaryOp::Plus => {
                self.out.push('+');
                prec::UNARY
            }
        };
        self.expr(&unary.operand, min)
    }

    fn binary(&mut self, binary: &Binary) -> Result<(), Error> {
        let p = binary.op.precedence();
        let (left, right) = if binary.op.is_comparison() {
            (prec::ADDITIVE, prec::ADDITIVE)
        } else {
            (p, p + 1)
        };
        self.expr(&binary.left, left)?;
        match binary.op {
            BinaryOp::Match => self.out.push_str(" : "),
            op => {
                self.out.push(' ');
                self.out.push_str(op.symbol());
                self.out.push(' ');
            }
        }
        self.expr(&binary.right, right)
    }

    fn in_list(&mut self, list: &InList) -> Result<(), Error> {
        self.expr(&list.expr, prec::ADDITIVE)?;
        self.out
            .push_str(if list.negated { " NOT IN (" } else { " IN (" });
        for (index, element) in list.list.iter().enumerate() {
            if index > 0 {
                self.out.push_str(", ");
            }
            let min = if is_predicate(element) {
                prec::COMPARISON + 1
            } else {
                prec::COMPARISON
            };
            let at = self.out.len();
            self.expr(element, min)?;
            // `IN (FROM` opens a subquery, so a first element that prints
            // starting with the word `from` is parenthesised.
            if index == 0 && starts_with_from(&self.out[at..]) {
                self.out.insert(at, '(');
                self.out.push(')');
            }
        }
        self.out.push(')');
        Ok(())
    }

    fn in_subquery(&mut self, in_query: &InSubquery) -> Result<(), Error> {
        self.expr(&in_query.expr, prec::ADDITIVE)?;
        self.out
            .push_str(if in_query.negated { " NOT IN " } else { " IN " });
        self.subquery(&in_query.query)
    }

    fn like(&mut self, like: &Like) -> Result<(), Error> {
        self.expr(&like.expr, prec::ADDITIVE)?;
        self.out.push_str(if like.negated { " NOT " } else { " " });
        self.out.push_str(match like.kind {
            LikeKind::Like => "LIKE ",
            LikeKind::RLike => "RLIKE ",
        });
        push_string(&mut self.out, &like.pattern);
        Ok(())
    }

    fn is_null(&mut self, is_null: &IsNull) -> Result<(), Error> {
        self.expr(&is_null.expr, prec::ADDITIVE)?;
        self.out.push_str(if is_null.negated {
            " IS NOT NULL"
        } else {
            " IS NULL"
        });
        Ok(())
    }

    fn cast(&mut self, cast: &Cast) -> Result<(), Error> {
        self.expr(&cast.expr, prec::CAST)?;
        self.out.push_str("::");
        self.out.push_str(&cast.type_name);
        Ok(())
    }

    fn array(&mut self, array: &ArrayLiteral) -> Result<(), Error> {
        self.out.push('[');
        self.exprs(&array.elements, 0)?;
        self.out.push(']');
        Ok(())
    }

    /// Prints an expression that holds no other expression.
    #[inline(never)]
    fn leaf(&mut self, expr: &Expr) {
        match expr {
            Expr::Literal(literal) => match &literal.value {
                LiteralValue::Null => self.out.push_str("null"),
                LiteralValue::Boolean(true) => self.out.push_str("true"),
                LiteralValue::Boolean(false) => self.out.push_str("false"),
                LiteralValue::Integer(text) | LiteralValue::Decimal(text) => {
                    self.out.push_str(text);
                }
                LiteralValue::String(value) => push_string(&mut self.out, value),
            },
            Expr::Interval(interval) => {
                let _ = write!(self.out, "{} {}", interval.value, interval.unit);
            }
            Expr::Field(name) => push_name(&mut self.out, name),
            Expr::Param(param) => push_param(&mut self.out, &param.name, param.identifier),
            _ => {}
        }
    }
}
