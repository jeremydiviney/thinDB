//! SQL public surface — re-exports the lexer + parser entry point.
//!
//! Typical usage:
//!   var arena = std.heap.ArenaAllocator.init(allocator);
//!   defer arena.deinit();
//!
//!   const root = try thindb.sql.parse(arena.allocator(),
//!       "SELECT id FROM users WHERE qty > 5 LIMIT 10");
//!   var q = try thindb.local.compile(allocator, db, root.*);
//!   defer q.deinit();
//!
//! The returned *ir.Op tree lives in the supplied arena. Pass it
//! straight to the existing IR→Query compile path (e.g. via
//! PlanBuilder.compile or local.compileWithSession).

const lexer = @import("lexer.zig");
const parser = @import("parser.zig");

pub const Token = lexer.Token;
pub const TokenTag = lexer.TokenTag;
pub const Lexer = lexer.Lexer;
pub const LexError = lexer.LexError;

pub const parse = parser.parse;
pub const parseDialect = parser.parseDialect;
pub const parseDialectWithUdfs = parser.parseDialectWithUdfs;
pub const Dialect = @import("../types.zig").Dialect;
pub const ParseError = parser.ParseError;
pub const MaterializeHint = parser.MaterializeHint;

test {
    _ = lexer;
    _ = parser;
}
