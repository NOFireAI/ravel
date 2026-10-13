//! Renders a syntax tree back to ES|QL text.

use std::fmt::Write as _;

use super::{
    BinaryOp, CallArgs, Command, Expr, Field, LikeKind, LiteralValue, NullsOrder, ParamName,
    QualifiedName, Query, SortOrder, UnaryOp, prec,
};
use crate::error::Error;
use crate::limits::{LimitKind, Limits};

/// Renders `query` as ES|QL text, adding parentheses only where precedence
/// requires them. When the printed text is itself within `limits`, parsing it
/// with the same limits yields a tree equal to `query` under
/// [`Query::eq_ignoring_spans`]. The printer adds spaces around operators and
/// writes a tab, newline, quote or backslash in a string as a two-byte escape,
/// so a statement at the byte limit can print to text past it; parsing that
/// text returns the [`LimitKind::StatementBytes`] limit error.
///
/// Refuses an expression nested deeper than [`Limits::max_depth`], because
/// the printer recurses once per level.
pub fn print(query: &Query, limits: &Limits) -> Result<String, Error> {
    let mut printer = Printer {
        out: String::new(),
        depth: 0,
        limits,
    };
    printer.command(&query.source)?;
    for command in &query.processing {
        printer.out.push_str(" | ");
        printer.command(command)?;
    }
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

fn push_name(out: &mut String, name: &QualifiedName) {
    for (index, part) in name.parts.iter().enumerate() {
        if index > 0 {
            out.push('.');
        }
        if part.quoted {
            out.push('`');
            out.push_str(&part.text.replace('`', "``"));
            out.push('`');
        } else {
            out.push_str(&part.text);
        }
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

impl Printer<'_> {
    fn command(&mut self, command: &Command) -> Result<(), Error> {
        match command {
            Command::Row(c) => {
                self.out.push_str("ROW ");
                self.fields(&c.fields)?;
            }
            Command::From(c) => {
                self.out.push_str("FROM ");
                for (index, source) in c.sources.iter().enumerate() {
                    if index > 0 {
                        self.out.push_str(", ");
                    }
                    if source.quoted {
                        push_string(&mut self.out, &source.text);
                    } else {
                        self.out.push_str(&source.text);
                    }
                }
                if let Some(metadata) = &c.metadata {
                    self.out.push_str(" METADATA ");
                    push_names(&mut self.out, &metadata.fields);
                }
            }
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
            Command::Stats(c) => {
                self.out.push_str("STATS");
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
            }
            Command::Sort(c) => {
                self.out.push_str("SORT ");
                for (index, key) in c.keys.iter().enumerate() {
                    if index > 0 {
                        self.out.push_str(", ");
                    }
                    self.expr(&key.expr, 0)?;
                    match key.order {
                        Some(SortOrder::Asc) => self.out.push_str(" ASC"),
                        Some(SortOrder::Desc) => self.out.push_str(" DESC"),
                        None => {}
                    }
                    match key.nulls {
                        Some(NullsOrder::First) => self.out.push_str(" NULLS FIRST"),
                        Some(NullsOrder::Last) => self.out.push_str(" NULLS LAST"),
                        None => {}
                    }
                }
            }
            Command::Limit(c) => {
                let _ = write!(self.out, "LIMIT {}", c.count);
            }
        }
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
        self.depth += 1;
        if self.depth > self.limits.max_depth {
            return Err(Error::LimitExceeded {
                limit: LimitKind::NestingDepth,
                max: self.limits.max_depth,
                span: expr.span(),
            });
        }
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

    fn expr_body(&mut self, expr: &Expr) -> Result<(), Error> {
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
            Expr::Call(call) => {
                self.out.push_str(&call.name);
                self.out.push('(');
                match &call.args {
                    CallArgs::Star => self.out.push('*'),
                    CallArgs::List(args) => self.exprs(args, 0)?,
                }
                self.out.push(')');
            }
            Expr::Unary(unary) => {
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
                self.expr(&unary.operand, min)?;
            }
            Expr::Binary(binary) => {
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
                self.expr(&binary.right, right)?;
            }
            Expr::In(list) => {
                self.expr(&list.expr, prec::ADDITIVE)?;
                self.out
                    .push_str(if list.negated { " NOT IN (" } else { " IN (" });
                self.exprs(&list.list, prec::COMPARISON)?;
                self.out.push(')');
            }
            Expr::Like(like) => {
                self.expr(&like.expr, prec::ADDITIVE)?;
                self.out.push_str(if like.negated { " NOT " } else { " " });
                self.out.push_str(match like.kind {
                    LikeKind::Like => "LIKE ",
                    LikeKind::RLike => "RLIKE ",
                });
                push_string(&mut self.out, &like.pattern);
            }
            Expr::IsNull(is_null) => {
                self.expr(&is_null.expr, prec::ADDITIVE)?;
                self.out.push_str(if is_null.negated {
                    " IS NOT NULL"
                } else {
                    " IS NULL"
                });
            }
            Expr::Cast(cast) => {
                self.expr(&cast.expr, prec::CAST)?;
                self.out.push_str("::");
                self.out.push_str(&cast.type_name);
            }
            Expr::Array(array) => {
                self.out.push('[');
                self.exprs(&array.elements, 0)?;
                self.out.push(']');
            }
        }
        Ok(())
    }
}
