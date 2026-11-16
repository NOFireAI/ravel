//! Structural guard over the token stream, run before parsing.
//!
//! A deep tree can exhaust a small worker stack in the parser, the printer,
//! or the tree's own drop. Bracket nesting and pipeline length are visible in
//! the token stream without parsing, so they are refused here, ahead of any
//! syntax error later in the statement. Nesting built from operators depends
//! on precedence and is bounded by the parser itself.

use crate::error::Error;
use crate::lexer::{Token, TokenKind};
use crate::limits::{LimitKind, Limits};

pub(crate) fn check(tokens: &[Token], limits: &Limits) -> Result<(), Error> {
    let mut brackets = 0usize;
    let mut stages = 0usize;
    for (index, token) in tokens.iter().enumerate() {
        if index == 0 || token.kind == TokenKind::Pipe {
            stages += 1;
            if stages > limits.max_pipeline_stages {
                return Err(Error::LimitExceeded {
                    limit: LimitKind::PipelineStages,
                    max: limits.max_pipeline_stages,
                    span: token.span,
                });
            }
        }
        match token.kind {
            TokenKind::LParen | TokenKind::LBracket | TokenKind::LBrace => {
                brackets += 1;
                // The expression holding the outermost bracket is level 1.
                if brackets + 1 > limits.max_depth {
                    return Err(Error::LimitExceeded {
                        limit: LimitKind::NestingDepth,
                        max: limits.max_depth,
                        span: token.span,
                    });
                }
            }
            TokenKind::RParen | TokenKind::RBracket | TokenKind::RBrace => {
                brackets = brackets.saturating_sub(1);
            }
            _ => {}
        }
    }
    Ok(())
}
