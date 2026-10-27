//! Minimal linear-time regular expression engine (Thompson NFA + Pike
//! VM with submatch tracking), in the RE2 family. No backtracking, so
//! matching is O(pattern × input) with no catastrophic blow-up (ReDoS).
//!
//! Supported syntax (the common POSIX/RE2 subset):
//!   literals, `.` (any byte except '\n'), `[...]` / `[^...]` classes
//!   with ranges and POSIX classes (`[[:alpha:]]`, `[[:^digit:]]`),
//!   shorthands `\d \w \s \D \W \S`, escapes (`\.` `\n` `\t` `\\`
//!   etc), quantifiers `* + ? {n} {n,} {n,m}` (greedy and lazy
//!   `*? +? ??`), alternation `|`, capturing `( )` and non-capturing
//!   `(?: )` groups, anchors `^ $` (the text's start and end, or any
//!   line's with `Options.multiline`), word boundaries `\b \B`.
//!
//! NOT supported (require backtracking — same omissions as RE2):
//!   in-pattern backreferences, lookahead/lookbehind. Backreferences in
//!   a *replacement* template (`\1`) are supported by `replaceAll` since
//!   that's plain substitution, not a pattern feature.
//!
//! Matching is byte-oriented (ASCII-aware). UTF-8 bytes flow through
//! `.` and negated classes fine. A Unicode property class (`\p{...}`,
//! `\P{...}`) is an invalid pattern, and non-ASCII case folding is out of
//! scope.

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const Error = error{RegexInvalidPattern} || Allocator.Error;

/// Pattern-wide flags, as SQL match types set them.
pub const Options = struct {
    /// ASCII letters match either case.
    case_insensitive: bool = false,
    /// `.` matches '\n' too.
    dot_all: bool = false,
    /// `^` and `$` also match just after and just before a '\n', not
    /// only at the text's start and end.
    multiline: bool = false,
};

/// 256-bit set of matchable bytes.
const CharClass = struct {
    bits: [32]u8 = [_]u8{0} ** 32,

    fn add(self: *CharClass, b: u8) void {
        self.bits[b >> 3] |= (@as(u8, 1) << @intCast(b & 7));
    }
    fn addRange(self: *CharClass, lo: u8, hi: u8) void {
        var c: usize = lo;
        while (c <= hi) : (c += 1) self.add(@intCast(c));
    }
    fn negate(self: *CharClass) void {
        for (&self.bits) |*x| x.* = ~x.*;
    }
    fn contains(self: *const CharClass, b: u8) bool {
        return (self.bits[b >> 3] & (@as(u8, 1) << @intCast(b & 7))) != 0;
    }
    fn addOtherCases(self: *CharClass) void {
        for ('a'..'z' + 1) |c| {
            const lower: u8 = @intCast(c);
            const upper = std.ascii.toUpper(lower);
            if (self.contains(lower) or self.contains(upper)) {
                self.add(lower);
                self.add(upper);
            }
        }
    }
};

const Inst = union(enum) {
    class: CharClass, // consume one byte in the class
    match,
    jmp: u32,
    split: struct { a: u32, b: u32 }, // try `a` first (higher priority)
    save: u32, // record current position into capture slot
    bol,
    eol,
    word_boundary,
    not_word_boundary,
};

// ---------------------------------------------------------------------------
// AST
// ---------------------------------------------------------------------------

const Node = union(enum) {
    empty,
    class: CharClass,
    bol,
    eol,
    word_boundary,
    not_word_boundary,
    concat: []const *Node,
    alt: []const *Node,
    repeat: struct { node: *Node, min: u32, max: ?u32, greedy: bool }, // max=null → unbounded
    group: struct { node: *Node, slot: ?u32 }, // slot=null → non-capturing
};

const Parser = struct {
    pat: []const u8,
    pos: usize = 0,
    arena: Allocator,
    next_group: u32 = 1, // group 0 is the whole match
    options: Options = .{},

    fn peek(self: *Parser) ?u8 {
        return if (self.pos < self.pat.len) self.pat[self.pos] else null;
    }
    fn advance(self: *Parser) u8 {
        const c = self.pat[self.pos];
        self.pos += 1;
        return c;
    }
    fn node(self: *Parser, v: Node) Error!*Node {
        const n = try self.arena.create(Node);
        n.* = v;
        return n;
    }

    fn literal(self: *Parser, b: u8) Error!*Node {
        var cc: CharClass = .{};
        cc.add(b);
        if (self.options.case_insensitive) cc.addOtherCases();
        return self.node(.{ .class = cc });
    }

    fn parseAlt(self: *Parser) Error!*Node {
        var branches: std.ArrayList(*Node) = .empty;
        try branches.append(self.arena, try self.parseConcat());
        while (self.peek() == '|') {
            _ = self.advance();
            try branches.append(self.arena, try self.parseConcat());
        }
        if (branches.items.len == 1) return branches.items[0];
        return self.node(.{ .alt = try branches.toOwnedSlice(self.arena) });
    }

    fn parseConcat(self: *Parser) Error!*Node {
        var parts: std.ArrayList(*Node) = .empty;
        while (self.peek()) |c| {
            if (c == '|' or c == ')') break;
            try parts.append(self.arena, try self.parseRepeat());
        }
        if (parts.items.len == 0) return self.node(.empty);
        if (parts.items.len == 1) return parts.items[0];
        return self.node(.{ .concat = try parts.toOwnedSlice(self.arena) });
    }

    fn parseRepeat(self: *Parser) Error!*Node {
        var atom = try self.parseAtom();
        while (self.peek()) |c| {
            var min: u32 = 0;
            var max: ?u32 = null;
            switch (c) {
                '*' => {
                    _ = self.advance();
                },
                '+' => {
                    _ = self.advance();
                    min = 1;
                },
                '?' => {
                    _ = self.advance();
                    max = 1;
                },
                '{' => {
                    const parsed = try self.parseBraces() orelse break;
                    min = parsed.min;
                    max = parsed.max;
                },
                else => break,
            }
            var greedy = true;
            if (self.peek() == '?') {
                _ = self.advance();
                greedy = false;
            }
            atom = try self.node(.{ .repeat = .{ .node = atom, .min = min, .max = max, .greedy = greedy } });
        }
        return atom;
    }

    /// A `{n}`, `{n,}` or `{n,m}` repeat at the cursor, or null with the
    /// cursor unmoved when the brace starts no repeat (`{,2}`, `{x}`), which
    /// then reads as a literal `{`, as in RE2.
    fn parseBraces(self: *Parser) Error!?struct { min: u32, max: ?u32 } {
        const start = self.pos;
        _ = self.advance(); // '{'
        if (!self.atDigit()) {
            self.pos = start;
            return null;
        }
        const min = try self.parseInt();
        var max: ?u32 = min;
        if (self.peek() == ',') {
            _ = self.advance();
            if (self.peek() == '}') {
                max = null; // {n,}
            } else if (self.atDigit()) {
                max = try self.parseInt();
            }
        }
        if (self.peek() != '}') {
            self.pos = start;
            return null;
        }
        _ = self.advance();
        return .{ .min = min, .max = max };
    }

    fn atDigit(self: *Parser) bool {
        const c = self.peek() orelse return false;
        return c >= '0' and c <= '9';
    }

    fn parseInt(self: *Parser) Error!u32 {
        const start = self.pos;
        while (self.peek()) |c| {
            if (c < '0' or c > '9') break;
            _ = self.advance();
        }
        if (self.pos == start) return Error.RegexInvalidPattern;
        return std.fmt.parseInt(u32, self.pat[start..self.pos], 10) catch Error.RegexInvalidPattern;
    }

    fn parseAtom(self: *Parser) Error!*Node {
        const c = self.peek() orelse return Error.RegexInvalidPattern;
        switch (c) {
            '(' => {
                _ = self.advance();
                var slot: ?u32 = null;
                if (self.peek() == '?') {
                    _ = self.advance();
                    if (self.peek() != ':') return Error.RegexInvalidPattern; // lookaround unsupported
                    _ = self.advance();
                } else {
                    slot = self.next_group;
                    self.next_group += 1;
                }
                const inner = try self.parseAlt();
                if (self.peek() != ')') return Error.RegexInvalidPattern;
                _ = self.advance();
                return self.node(.{ .group = .{ .node = inner, .slot = slot } });
            },
            '[' => return self.parseClass(),
            '.' => {
                _ = self.advance();
                var cc: CharClass = .{};
                if (!self.options.dot_all) cc.add('\n');
                cc.negate();
                return self.node(.{ .class = cc });
            },
            '^' => {
                _ = self.advance();
                return self.node(.bol);
            },
            '$' => {
                _ = self.advance();
                return self.node(.eol);
            },
            '\\' => return self.parseEscape(),
            ')', '*', '+', '?' => return Error.RegexInvalidPattern,
            '{' => {
                // A repeat with nothing to repeat.
                if ((try self.parseBraces()) != null) return Error.RegexInvalidPattern;
                _ = self.advance();
                return self.literal(c);
            },
            else => {
                _ = self.advance();
                return self.literal(c);
            },
        }
    }

    fn parseEscape(self: *Parser) Error!*Node {
        _ = self.advance(); // backslash
        const c = self.peek() orelse return Error.RegexInvalidPattern;
        _ = self.advance();
        switch (c) {
            'b' => return self.node(.word_boundary),
            'B' => return self.node(.not_word_boundary),
            'd', 'D', 'w', 'W', 's', 'S' => {
                return self.node(.{ .class = shorthandClass(c) });
            },
            else => return self.literal(try escapedByte(c)),
        }
    }

    fn parseClass(self: *Parser) Error!*Node {
        _ = self.advance(); // '['
        var cc: CharClass = .{};
        var negated = false;
        if (self.peek() == '^') {
            _ = self.advance();
            negated = true;
        }
        var first = true;
        while (self.peek()) |c| {
            if (c == ']' and !first) {
                _ = self.advance();
                if (self.options.case_insensitive) cc.addOtherCases();
                if (negated) cc.negate();
                return self.node(.{ .class = cc });
            }
            first = false;
            if (try self.parsePosixClass(&cc)) continue;
            var lo: u8 = undefined;
            if (c == '\\') {
                _ = self.advance();
                const e = self.peek() orelse return Error.RegexInvalidPattern;
                _ = self.advance();
                switch (e) {
                    'd', 'D', 'w', 'W', 's', 'S' => {
                        const sub = shorthandClass(e);
                        for (0..256) |b| if (sub.contains(@intCast(b))) cc.add(@intCast(b));
                        continue;
                    },
                    else => lo = try escapedByte(e),
                }
            } else {
                lo = self.advance();
            }
            // Range?  lo '-' hi  (but a trailing '-' before ']' is literal)
            if (self.peek() == '-' and self.pos + 1 < self.pat.len and self.pat[self.pos + 1] != ']') {
                _ = self.advance(); // '-'
                var hi: u8 = undefined;
                if (self.peek() == '\\') {
                    _ = self.advance();
                    const e = self.peek() orelse return Error.RegexInvalidPattern;
                    _ = self.advance();
                    hi = try escapedByte(e);
                } else {
                    hi = self.advance();
                }
                if (hi < lo) return Error.RegexInvalidPattern;
                cc.addRange(lo, hi);
            } else {
                cc.add(lo);
            }
        }
        return Error.RegexInvalidPattern; // unterminated class
    }

    /// Adds the POSIX class at the cursor, such as `[:alpha:]` or its
    /// complement `[:^alpha:]`, to `cc`, and says whether there was one.
    fn parsePosixClass(self: *Parser, cc: *CharClass) Error!bool {
        const rest = self.pat[self.pos..];
        if (!std.mem.startsWith(u8, rest, "[:")) return false;
        const close = std.mem.indexOfPos(u8, rest, 2, ":]") orelse return false;
        const negated = rest[2] == '^';
        const name = rest[if (negated) 3 else 2..close];
        const has = for (POSIX_CLASSES) |pc| {
            if (std.mem.eql(u8, pc.name, name)) break pc.has;
        } else return Error.RegexInvalidPattern;
        for (0..256) |b| if (has(@intCast(b)) != negated) cc.add(@intCast(b));
        self.pos += close + 2;
        return true;
    }
};

const POSIX_CLASSES = [_]struct { name: []const u8, has: *const fn (u8) bool }{
    .{ .name = "alnum", .has = std.ascii.isAlphanumeric },
    .{ .name = "alpha", .has = std.ascii.isAlphabetic },
    .{ .name = "ascii", .has = std.ascii.isAscii },
    .{ .name = "blank", .has = isBlank },
    .{ .name = "cntrl", .has = std.ascii.isControl },
    .{ .name = "digit", .has = std.ascii.isDigit },
    .{ .name = "graph", .has = isGraph },
    .{ .name = "lower", .has = std.ascii.isLower },
    .{ .name = "print", .has = std.ascii.isPrint },
    .{ .name = "punct", .has = isPunct },
    .{ .name = "space", .has = std.ascii.isWhitespace },
    .{ .name = "upper", .has = std.ascii.isUpper },
    .{ .name = "word", .has = isWordByte },
    .{ .name = "xdigit", .has = std.ascii.isHex },
};

fn isBlank(b: u8) bool {
    return b == ' ' or b == '\t';
}

fn isGraph(b: u8) bool {
    return std.ascii.isPrint(b) and b != ' ';
}

fn isPunct(b: u8) bool {
    return isGraph(b) and !std.ascii.isAlphanumeric(b);
}

/// The byte `\c` stands for. A Unicode property class would need classes
/// over UTF-8 sequences, so `\p` and `\P` refuse the pattern rather than
/// read as the letter.
fn escapedByte(c: u8) Error!u8 {
    if (c == 'p' or c == 'P') return Error.RegexInvalidPattern;
    return unescapeByte(c);
}

fn unescapeByte(c: u8) u8 {
    return switch (c) {
        'n' => '\n',
        't' => '\t',
        'r' => '\r',
        'f' => 0x0c,
        'v' => 0x0b,
        '0' => 0,
        else => c, // \. \\ \/ \( etc → the literal byte
    };
}

fn shorthandClass(c: u8) CharClass {
    var cc: CharClass = .{};
    switch (c) {
        'd', 'D' => cc.addRange('0', '9'),
        'w', 'W' => {
            cc.addRange('a', 'z');
            cc.addRange('A', 'Z');
            cc.addRange('0', '9');
            cc.add('_');
        },
        's', 'S' => {
            cc.add(' ');
            cc.add('\t');
            cc.add('\n');
            cc.add('\r');
            cc.add(0x0c);
            cc.add(0x0b);
        },
        else => unreachable,
    }
    if (c == 'D' or c == 'W' or c == 'S') cc.negate();
    return cc;
}

fn isWordByte(b: u8) bool {
    return (b >= 'a' and b <= 'z') or (b >= 'A' and b <= 'Z') or (b >= '0' and b <= '9') or b == '_';
}

/// Bytes a match can begin with, for the unanchored first-byte prefilter.
/// Returns an owned slice of those bytes when the set is small and useful
/// (1..=8 distinct bytes, no empty match, no leading assertion); otherwise
/// an empty slice (prefilter disabled). Conservatively over-approximates:
/// any byte returned is a *possible* start, never a missed one.
fn computePrefilter(allocator: Allocator, prog: []const Inst) Error![]u8 {
    const visited = try allocator.alloc(bool, prog.len);
    defer allocator.free(visited);
    @memset(visited, false);

    var stack: std.ArrayListUnmanaged(u32) = .empty;
    defer stack.deinit(allocator);
    try stack.append(allocator, 0);

    var set: CharClass = .{};
    var ok = true;
    while (stack.pop()) |pc| {
        if (visited[pc]) continue;
        visited[pc] = true;
        switch (prog[pc]) {
            .jmp => |t| try stack.append(allocator, t),
            .split => |s| {
                try stack.append(allocator, s.a);
                try stack.append(allocator, s.b);
            },
            .save => try stack.append(allocator, pc + 1),
            .class => |cc| {
                var b: usize = 0;
                while (b < 256) : (b += 1) if (cc.contains(@intCast(b))) set.add(@intCast(b));
            },
            // A reachable match means the empty string matches (start byte is
            // unconstrained); a reachable assertion makes the start
            // position-dependent. Either way, don't prefilter.
            .match, .bol, .eol, .word_boundary, .not_word_boundary => ok = false,
        }
    }
    if (!ok) return allocator.alloc(u8, 0);

    var count: usize = 0;
    for (set.bits) |m| count += @popCount(m);
    if (count == 0 or count > 8) return allocator.alloc(u8, 0);

    const bytes = try allocator.alloc(u8, count);
    var n: usize = 0;
    var b: usize = 0;
    while (b < 256) : (b += 1) if (set.contains(@intCast(b))) {
        bytes[n] = @intCast(b);
        n += 1;
    };
    return bytes;
}

/// How the backtracker crosses a greedy single-class star — the compiled
/// shape `L: split(L+1, L+3); class C; jmp L` — in one scan rather than
/// one explore step per byte.
const StarScan = union(enum) {
    none,
    /// At most four bytes leave C: memchr each, the earliest ends the run.
    breakers: struct { n: u8, bytes: [4]u8 },
    /// Too many bytes leave C for memchr: test membership byte by byte.
    dense,
};

fn starScanAt(prog: []const Inst, pc: usize) StarScan {
    if (pc + 2 >= prog.len) return .none;
    const s = switch (prog[pc]) {
        .split => |s| s,
        else => return .none,
    };
    if (s.a != pc + 1 or s.b != pc + 3) return .none;
    const cc = switch (prog[pc + 1]) {
        .class => |cc| cc,
        else => return .none,
    };
    switch (prog[pc + 2]) {
        .jmp => |t| if (t != pc) return .none,
        else => return .none,
    }
    var bytes: [4]u8 = undefined;
    var n: u8 = 0;
    for (0..256) |b| {
        if (cc.contains(@intCast(b))) continue;
        if (n == bytes.len) return .dense;
        bytes[n] = @intCast(b);
        n += 1;
    }
    return .{ .breakers = .{ .n = n, .bytes = bytes } };
}

/// The one byte a class matches, or null when it matches several or none.
fn singleByte(inst: Inst) ?u8 {
    const cc = switch (inst) {
        .class => |cc| cc,
        else => return null,
    };
    var count: usize = 0;
    for (cc.bits) |m| count += @popCount(m);
    if (count != 1) return null;
    for (cc.bits, 0..) |m, i| {
        if (m != 0) return @intCast(i * 8 + @ctz(m));
    }
    return null;
}

/// Bytes on which the program from `pc` can take its first step at a
/// position inside the input, or null when that also depends on something
/// besides the byte there (a reachable match, `^`, `\b`, `\B`). It may be a
/// superset: it only rules out alternatives that are bound to fail.
fn firstBytes(allocator: Allocator, prog: []const Inst, pc: u32, multiline: bool) Error!?CharClass {
    const seen = try allocator.alloc(bool, prog.len);
    defer allocator.free(seen);
    @memset(seen, false);
    var stack: std.ArrayListUnmanaged(u32) = .empty;
    defer stack.deinit(allocator);
    try stack.append(allocator, pc);

    var set: CharClass = .{};
    while (stack.pop()) |at| {
        if (seen[at]) continue;
        seen[at] = true;
        switch (prog[at]) {
            .jmp => |t| try stack.append(allocator, t),
            .split => |s| {
                try stack.append(allocator, s.a);
                try stack.append(allocator, s.b);
            },
            .save => try stack.append(allocator, at + 1),
            .class => |cc| for (&set.bits, cc.bits) |*x, y| {
                x.* |= y;
            },
            .eol => if (multiline) set.add('\n'),
            .match, .bol, .word_boundary, .not_word_boundary => return null,
        }
    }
    return set;
}

const BtStep = struct {
    /// This pc's row in the visited set. Null when pc is entered only by
    /// falling through from pc-1: its pair is then explored exactly when
    /// pc-1's is, so it needs no bit of its own.
    row: ?u32,
    /// Single-byte classes from pc on that fall through to one another,
    /// matched as one literal compare; 0 when prog[pc] is not one.
    lit_len: u32,
    star: StarScan,
    /// For a split that is not a star: the `BtPlan.guards` entry holding
    /// the bytes its second branch can begin with inside the input, or
    /// null when that branch must always be kept.
    alt_guard: ?u32,
    /// For a star: false when its continuation cannot begin on a byte of
    /// the star's class, so no position inside a crossed run is retried.
    star_tail: bool,
};

/// What the backtracker precomputes per program.
const BtPlan = struct {
    steps: []BtStep,
    /// `lit_bytes[pc]` is the byte the single-byte class at pc matches, so
    /// a literal run is `lit_bytes[pc..][0..lit_len]`.
    lit_bytes: []u8,
    guards: []CharClass,
    rows: u32,

    fn init(allocator: Allocator, prog: []const Inst, multiline: bool) Error!BtPlan {
        const is_target = try allocator.alloc(bool, prog.len);
        defer allocator.free(is_target);
        @memset(is_target, false);
        is_target[0] = true;
        for (prog) |inst| switch (inst) {
            .jmp => |t| is_target[t] = true,
            .split => |s| {
                is_target[s.a] = true;
                is_target[s.b] = true;
            },
            else => {},
        };

        const steps = try allocator.alloc(BtStep, prog.len);
        errdefer allocator.free(steps);
        const lit_bytes = try allocator.alloc(u8, prog.len);
        errdefer allocator.free(lit_bytes);

        var guards: std.ArrayListUnmanaged(CharClass) = .empty;
        errdefer guards.deinit(allocator);
        var rows: u32 = 0;
        for (steps, 0..) |*step, pc| {
            step.row = if (is_target[pc]) rows else null;
            if (is_target[pc]) rows += 1;
            step.star = starScanAt(prog, pc);
            step.alt_guard = null;
            step.star_tail = true;
            const s = switch (prog[pc]) {
                .split => |s| s,
                else => continue,
            };
            const first = try firstBytes(allocator, prog, s.b, multiline) orelse continue;
            if (step.star == .none) {
                step.alt_guard = @intCast(guards.items.len);
                try guards.append(allocator, first);
            } else {
                const body = prog[pc + 1].class;
                var overlap: u8 = 0;
                for (first.bits, body.bits) |x, y| overlap |= x & y;
                step.star_tail = overlap != 0;
            }
        }
        var pc = prog.len;
        while (pc > 0) {
            pc -= 1;
            const byte = singleByte(prog[pc]);
            lit_bytes[pc] = byte orelse 0;
            steps[pc].lit_len = if (byte == null) 0 else blk: {
                const extends = pc + 1 < prog.len and !is_target[pc + 1];
                break :blk 1 + if (extends) steps[pc + 1].lit_len else 0;
            };
        }
        return .{ .steps = steps, .lit_bytes = lit_bytes, .guards = try guards.toOwnedSlice(allocator), .rows = rows };
    }

    fn deinit(self: BtPlan, allocator: Allocator) void {
        allocator.free(self.steps);
        allocator.free(self.lit_bytes);
        allocator.free(self.guards);
    }
};

/// First index `>= from` whose byte is outside the star's class, or
/// `input.len`.
fn starRunEnd(scan: StarScan, cc: *const CharClass, input: []const u8, from: usize) usize {
    switch (scan) {
        .breakers => |*b| return findAnyByte(input, from, b.bytes[0..b.n]),
        .dense => {
            var j = from;
            while (j < input.len and cc.contains(input[j])) j += 1;
            return j;
        },
        .none => unreachable,
    }
}

/// First index `>= from` holding any of `bytes`, or `input.len`, in one
/// pass for all of them. The chunks are 16 bytes because values are short:
/// `std.mem.indexOfScalarPos` uses the widest vector and finishes a short
/// value byte by byte, while here the last chunk instead re-reads bytes
/// overlapping the previous one.
fn findAnyByte(input: []const u8, from: usize, bytes: []const u8) usize {
    const V = @Vector(16, u8);
    if (input.len < 16) {
        for (input[from..], from..) |c, j| {
            for (bytes) |b| if (c == b) return j;
        }
        return input.len;
    }
    var i = from;
    while (i < input.len) {
        const at = @min(i, input.len - 16);
        const chunk: V = input[at..][0..16].*;
        var hits: u16 = 0;
        for (bytes) |b| hits |= @as(u16, @bitCast(chunk == @as(V, @splat(b))));
        hits &= ~@as(u16, 0) << @intCast(i - at);
        if (hits != 0) return at + @ctz(hits);
        i = at + 16;
    }
    return input.len;
}

/// The backtracker's pending jobs. It pushes one every few steps, so the
/// stack lives in locals rather than behind the scratch pointer, where every
/// store through `caps` or `visited` would force its length back to memory.
const JobStack = struct {
    buf: []BtJob,
    len: usize = 0,

    inline fn push(self: *JobStack, scratch: *Scratch, job: BtJob) Error!void {
        if (self.len == self.buf.len) self.buf = try growJobs(scratch, self.len);
        self.buf[self.len] = job;
        self.len += 1;
    }
};

fn growJobs(scratch: *Scratch, len: usize) Error![]BtJob {
    scratch.bt_jobs.items.len = len;
    try scratch.bt_jobs.ensureUnusedCapacity(scratch.backing, 1);
    return scratch.bt_jobs.allocatedSlice();
}

/// Written slot by slot: building each `?usize` whole goes through a stack
/// temporary whose reload stalls store forwarding.
inline fn copyCaps(out: []?usize, caps: []const ?u32) void {
    for (out, caps) |*o, cap| {
        if (cap) |c| o.* = c else o.* = null;
    }
}

/// Equality for the few bytes of a literal run, where `std.mem.eql`'s
/// out-of-line vector routine costs more than the compare.
inline fn bytesEqual(a: []const u8, b: []const u8) bool {
    for (a, b) |x, y| if (x != y) return false;
    return true;
}

/// First set bit in `[lo, hi)`, or null.
fn bitsFirstSet(words: []const u64, lo: usize, hi: usize) ?usize {
    var i = lo;
    while (i < hi) {
        const off: u6 = @intCast(i & 63);
        const span: usize = @min(64 - @as(usize, off), hi - i);
        var word = words[i >> 6] >> off;
        if (span < 64) word &= (@as(u64, 1) << @intCast(span)) - 1;
        if (word != 0) return i + @ctz(word);
        i += span;
    }
    return null;
}

fn bitsSetRange(words: []u64, lo: usize, hi: usize) void {
    var i = lo;
    while (i < hi) {
        const off: u6 = @intCast(i & 63);
        const span: usize = @min(64 - @as(usize, off), hi - i);
        const ones: u64 = if (span == 64) ~@as(u64, 0) else (@as(u64, 1) << @intCast(span)) - 1;
        words[i >> 6] |= ones << off;
        i += span;
    }
}

// ---------------------------------------------------------------------------
// Compiler: AST → flat instruction program
// ---------------------------------------------------------------------------

const Compiler = struct {
    prog: std.ArrayList(Inst) = .empty,
    allocator: Allocator,

    fn emit(self: *Compiler, inst: Inst) Error!u32 {
        const idx: u32 = @intCast(self.prog.items.len);
        try self.prog.append(self.allocator, inst);
        return idx;
    }

    fn compile(self: *Compiler, n: *const Node) Error!void {
        switch (n.*) {
            .empty => {},
            .class => |cc| _ = try self.emit(.{ .class = cc }),
            .bol => _ = try self.emit(.bol),
            .eol => _ = try self.emit(.eol),
            .word_boundary => _ = try self.emit(.word_boundary),
            .not_word_boundary => _ = try self.emit(.not_word_boundary),
            .concat => |parts| for (parts) |p| try self.compile(p),
            .group => |g| {
                if (g.slot) |s| _ = try self.emit(.{ .save = 2 * s });
                try self.compile(g.node);
                if (g.slot) |s| _ = try self.emit(.{ .save = 2 * s + 1 });
            },
            .alt => |branches| try self.compileAlt(branches),
            .repeat => |r| try self.compileRepeat(r.node, r.min, r.max, r.greedy),
        }
    }

    fn compileAlt(self: *Compiler, branches: []const *Node) Error!void {
        // split L0,(split L1,(...)); each branch jumps to a shared end.
        var end_jmps: std.ArrayList(u32) = .empty;
        defer end_jmps.deinit(self.allocator);
        for (branches, 0..) |b, i| {
            const last = i + 1 == branches.len;
            var split_idx: u32 = 0;
            if (!last) split_idx = try self.emit(.{ .split = .{ .a = 0, .b = 0 } });
            const branch_start: u32 = @intCast(self.prog.items.len);
            try self.compile(b);
            if (!last) {
                const j = try self.emit(.{ .jmp = 0 });
                try end_jmps.append(self.allocator, j);
                self.prog.items[split_idx].split = .{ .a = branch_start, .b = @intCast(self.prog.items.len) };
            }
        }
        const end: u32 = @intCast(self.prog.items.len);
        for (end_jmps.items) |j| self.prog.items[j].jmp = end;
    }

    fn compileRepeat(self: *Compiler, n: *const Node, min: u32, max: ?u32, greedy: bool) Error!void {
        // Mandatory copies.
        var i: u32 = 0;
        while (i < min) : (i += 1) try self.compile(n);
        if (max) |m| {
            // Optional copies: (m - min) of `node?`.
            var k: u32 = min;
            while (k < m) : (k += 1) try self.compileQuest(n, greedy);
        } else {
            // Unbounded tail. min copies done; now a star (if min==0) or
            // the last mandatory copy was already emitted, add a star.
            if (min == 0) {
                try self.compileStar(n, greedy);
            } else {
                // a+ semantics: re-loop on the already-required structure.
                // Emit one more `node*` so total is node{min}node* = node{min,}.
                try self.compileStar(n, greedy);
            }
        }
    }

    fn compileQuest(self: *Compiler, n: *const Node, greedy: bool) Error!void {
        const split_idx = try self.emit(.{ .split = .{ .a = 0, .b = 0 } });
        const body: u32 = @intCast(self.prog.items.len);
        try self.compile(n);
        const after: u32 = @intCast(self.prog.items.len);
        self.prog.items[split_idx].split = if (greedy) .{ .a = body, .b = after } else .{ .a = after, .b = body };
    }

    fn compileStar(self: *Compiler, n: *const Node, greedy: bool) Error!void {
        const l1 = try self.emit(.{ .split = .{ .a = 0, .b = 0 } });
        const body: u32 = @intCast(self.prog.items.len);
        try self.compile(n);
        _ = try self.emit(.{ .jmp = l1 });
        const after: u32 = @intCast(self.prog.items.len);
        self.prog.items[l1].split = if (greedy) .{ .a = body, .b = after } else .{ .a = after, .b = body };
    }
};

// ---------------------------------------------------------------------------
// Compiled regex + Pike VM
// ---------------------------------------------------------------------------

/// One epsilon-closed NFA thread: a program counter plus the capture slots
/// recorded so far. `caps` is never mutated in place — `.save` dups first —
/// so threads can share a buffer until they diverge.
const Thread = struct { pc: u32, caps: []?usize };

/// Most bits the backtracker's visited set may span (32 KiB); a larger
/// visited-rows × window product goes to the Pike VM.
const BACKTRACK_MAX_BITS: usize = 256 * 1024;

/// Positions are u32 to keep a job at 16 bytes; `findBacktrack` leaves
/// longer inputs to the Pike VM.
const BtJob = union(enum) {
    explore: struct { pc: u32, pos: u32 },
    restore: struct { slot: u32, old: ?u32 },
    /// Continuation `pc` of a crossed greedy star, still to be tried at
    /// positions `hi` down to `lo`.
    star_tail: struct { pc: u32, lo: u32, hi: u32 },
};

/// Reusable matcher scratch. Construct one per batch and pass it to every
/// `findWith` / `replaceAllScratch` call: the visited-stamp array, the two
/// thread lists, the seed buffer, and the per-match capture arena are all
/// retained across calls, so applying one compiled pattern to millions of
/// rows allocates essentially nothing in steady state.
pub const Scratch = struct {
    backing: Allocator,
    /// Lazily-built DFA for the pattern this scratch is serving (keyed by
    /// program identity below). Lives in the scratch — not the Regex — so
    /// the Regex stays immutable/shareable and the DFA amortizes across all
    /// the rows of a batch.
    dfa: ?Dfa = null,
    dfa_prog: usize = 0,
    /// Per-match capture arrays. Reset (retain_capacity) at the start of
    /// each `find`, so capture dups cost a bump-alloc into reused memory.
    arena: std.heap.ArenaAllocator,
    /// `visited[pc] == gen` means pc was already closed this step. `gen`
    /// is monotonic across every step of every find sharing this scratch,
    /// which is why `visited` never needs re-zeroing between finds.
    visited: []u32 = &.{},
    gen: u32 = 0,
    clist: std.ArrayListUnmanaged(Thread) = .empty,
    carried: std.ArrayListUnmanaged(Thread) = .empty,
    /// Closure of `carried` at the next position, used once per run to test
    /// whether a class loop is stationary (safe to bulk-skip).
    probe: std.ArrayListUnmanaged(Thread) = .empty,
    /// All-null start ("seed") capture buffer, read-only during a find.
    seed: []?usize = &.{},
    /// Result slot buffer for `replaceAllScratch`'s internal `find` calls.
    slots: []?usize = &.{},
    /// Reused output buffer for `replaceAllScratch`: the result of each row
    /// is built here and borrowed by the caller, so a whole batch needs no
    /// per-row result allocation.
    out_buf: std.ArrayList(u8) = .empty,
    /// Backtracker state: one visited bit per (pc, position) of the
    /// searched window, the pending alternatives and capture restores, and
    /// the captures of the path being explored.
    bt_visited: []u64 = &.{},
    bt_jobs: std.ArrayListUnmanaged(BtJob) = .empty,
    bt_caps: []?u32 = &.{},
    /// How often the search without a memo ran out of budget for the
    /// program at `bt_prog`, out of `bt_tries` attempts.
    bt_prog: usize = 0,
    bt_tries: u64 = 0,
    bt_overruns: u64 = 0,

    pub fn init(backing: Allocator) Scratch {
        return .{ .backing = backing, .arena = std.heap.ArenaAllocator.init(backing) };
    }

    pub fn deinit(self: *Scratch) void {
        if (self.dfa) |*d| d.deinit();
        self.arena.deinit();
        if (self.visited.len > 0) self.backing.free(self.visited);
        if (self.seed.len > 0) self.backing.free(self.seed);
        if (self.slots.len > 0) self.backing.free(self.slots);
        self.clist.deinit(self.backing);
        self.carried.deinit(self.backing);
        self.probe.deinit(self.backing);
        self.out_buf.deinit(self.backing);
        self.backing.free(self.bt_visited);
        self.bt_jobs.deinit(self.backing);
        self.backing.free(self.bt_caps);
    }

    /// Whether to start `prog`'s backtracking search without a memo: yes
    /// until 64 tries show more than one in eight running out of budget.
    fn tryUnmemoized(self: *Scratch, prog: []const Inst) bool {
        if (self.bt_prog != @intFromPtr(prog.ptr)) {
            self.bt_prog = @intFromPtr(prog.ptr);
            self.bt_tries = 0;
            self.bt_overruns = 0;
        }
        if (self.bt_tries >= 64 and self.bt_overruns * 8 > self.bt_tries) return false;
        self.bt_tries += 1;
        return true;
    }

    /// Grow `visited`/`seed` to fit this program, and guard `gen` against
    /// wrapping (re-zero `visited` and restart `gen` if it's about to).
    fn prepare(self: *Scratch, prog_len: usize, n_slots: u32, input_len: usize) Error!void {
        if (self.visited.len < prog_len) {
            self.visited = try self.backing.realloc(self.visited, prog_len);
            @memset(self.visited, 0);
            self.gen = 0;
        }
        if (self.seed.len < n_slots) {
            self.seed = try self.backing.realloc(self.seed, n_slots);
        }
        @memset(self.seed[0..n_slots], null);
        // Up to two `gen` bumps per input position (one per step, one per
        // stationary-run probe), plus closure headroom.
        const headroom: u64 = 2 * @as(u64, input_len) + prog_len + 2;
        if (@as(u64, self.gen) + headroom >= std.math.maxInt(u32)) {
            @memset(self.visited, 0);
            self.gen = 0;
        }
    }
};

pub const Regex = struct {
    prog: []Inst,
    n_slots: u32, // 2 * (n_groups + 1)
    allocator: Allocator,
    /// `Options.multiline`: `.bol`/`.eol` hold at line breaks too.
    multiline: bool,
    /// True when the program is `^`-anchored at the start (prog[1] is a
    /// `.bol` — no top-level alternation). Lets `find` seed the start
    /// thread only where `.bol` holds instead of at every position, which
    /// is the dominant per-row cost for anchored patterns over many rows.
    anchored_start: bool,
    /// True when the program contains a `\b`/`\B` assertion. These make a
    /// thread's epsilon-closure depend on the surrounding bytes, which
    /// would break the stationary-run bulk-skip's position-independence
    /// assumption — so the bulk-skip is disabled when this is set.
    has_wordbound: bool,
    /// `loopy[pc]` is true for a `.class` instruction that sits inside a
    /// `*`/`+` repetition (a back-edge spans it). Only such classes can
    /// drive a stationary run, so the bulk-skip is only *attempted* when a
    /// surviving thread is one — which avoids paying the stationarity probe
    /// on every char of a non-repeating prefix like `https?://`.
    loopy: []const bool,
    /// For an *unanchored* pattern whose match can only begin with one of a
    /// small set of bytes (a literal/class prefix, e.g. `google`, `utm`),
    /// these are those bytes. `find` then memchr-skips straight to the next
    /// candidate start instead of seeding + stepping the NFA at every dead
    /// position. Empty when no useful prefix exists (anchored, can match
    /// empty, leading assertion, or too many distinct first bytes).
    prefilter_bytes: []const u8,
    bt: BtPlan,

    pub fn compile(allocator: Allocator, pattern: []const u8) Error!Regex {
        return compileWith(allocator, pattern, .{});
    }

    pub fn compileWith(allocator: Allocator, pattern: []const u8, options: Options) Error!Regex {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();

        var parser = Parser{ .pat = pattern, .arena = arena.allocator(), .options = options };
        const root = try parser.parseAlt();
        if (parser.pos != pattern.len) return Error.RegexInvalidPattern; // trailing junk, e.g. unmatched ')'

        var comp = Compiler{ .allocator = allocator };
        errdefer comp.prog.deinit(allocator);
        _ = try comp.emit(.{ .save = 0 });
        try comp.compile(root);
        _ = try comp.emit(.{ .save = 1 });
        _ = try comp.emit(.match);

        const prog = try comp.prog.toOwnedSlice(allocator);
        errdefer allocator.free(prog);
        var has_wordbound = false;
        for (prog) |inst| switch (inst) {
            .word_boundary, .not_word_boundary => has_wordbound = true,
            else => {},
        };

        // A `.class` is "loopy" if some back-edge (a jmp/split at index i
        // targeting t <= i) spans it — i.e. it lives in a `*`/`+` body.
        const loopy = try allocator.alloc(bool, prog.len);
        errdefer allocator.free(loopy);
        @memset(loopy, false);
        for (prog, 0..) |inst, i| {
            const back: ?u32 = switch (inst) {
                .jmp => |t| if (t <= i) t else null,
                .split => |s| blk: {
                    var lo: ?u32 = null;
                    if (s.a <= i) lo = s.a;
                    if (s.b <= i and (lo == null or s.b < lo.?)) lo = s.b;
                    break :blk lo;
                },
                else => null,
            };
            if (back) |t| {
                var pc = t;
                while (pc <= i) : (pc += 1) {
                    if (prog[pc] == .class) loopy[pc] = true;
                }
            }
        }

        const anchored_start = prog.len > 1 and prog[1] == .bol;
        const prefilter_bytes = if (anchored_start)
            try allocator.alloc(u8, 0)
        else
            try computePrefilter(allocator, prog);
        errdefer allocator.free(prefilter_bytes);

        const bt = try BtPlan.init(allocator, prog, options.multiline);

        return .{
            .prog = prog,
            .n_slots = 2 * parser.next_group,
            .allocator = allocator,
            .multiline = options.multiline,
            .anchored_start = anchored_start,
            .has_wordbound = has_wordbound,
            .loopy = loopy,
            .prefilter_bytes = prefilter_bytes,
            .bt = bt,
        };
    }

    pub fn deinit(self: *Regex) void {
        self.allocator.free(self.prog);
        self.allocator.free(self.loopy);
        self.allocator.free(self.prefilter_bytes);
        self.bt.deinit(self.allocator);
    }

    /// Leftmost match at or after `start`. Returns capture slots (slot
    /// 2k/2k+1 = start/end of group k; group 0 = whole match) into
    /// `out_slots` (len must be >= n_slots). Returns false on no match.
    /// `run_alloc` backs per-thread capture arrays; caller should pass an
    /// arena it can reset.
    pub fn find(self: *const Regex, run_alloc: Allocator, input: []const u8, start: usize, out_slots: []?usize) Error!bool {
        var scratch = Scratch.init(run_alloc);
        defer scratch.deinit();
        return self.findWith(&scratch, input, start, out_slots);
    }

    /// Like `find`, but using a caller-owned `Scratch` reused across calls.
    /// This is the form the batch kernels use: the visited array, thread
    /// lists, seed buffer, and capture arena all persist between rows.
    pub fn findWith(self: *const Regex, scratch: *Scratch, input: []const u8, start: usize, out_slots: []?usize) Error!bool {
        // DFA guard for the Pike VM's pathological shape: an unanchored
        // pattern with no usable first-byte prefilter seeds a thread at EVERY
        // input position. The lazy DFA decides match-existence in one linear
        // table-lookup pass, so a no-match row skips the VM entirely; rows
        // that do match pay one bounded extra scan. Anchored or prefiltered
        // patterns already fail fast in the VM, so the guard skips them.
        if (!self.anchored_start and !self.has_wordbound and self.prefilter_bytes.len == 0) {
            if (try self.dfaMatches(scratch, input, start)) |exists| {
                if (!exists) return false;
            }
        }
        if (try self.findBacktrack(scratch, input, start, out_slots)) |found| return found;
        return self.findWithImpl(true, scratch, input, start, out_slots);
    }

    /// Exact "does any match exist at or after `start`" — no captures, no
    /// match position. Runs the lazy DFA (one table lookup per byte, no
    /// thread lists or capture bookkeeping) when the pattern is eligible;
    /// falls back to the Pike VM (word boundaries, state-cache overflow).
    /// The DFA recognizes exactly the VM's language, so the answer is
    /// identical either way.
    pub fn matchesWith(self: *const Regex, scratch: *Scratch, input: []const u8, start: usize) Error!bool {
        if (try self.dfaMatches(scratch, input, start)) |exists| return exists;
        if (scratch.slots.len < self.n_slots) {
            scratch.slots = try scratch.backing.realloc(scratch.slots, self.n_slots);
        }
        return self.findWithImpl(true, scratch, input, start, scratch.slots[0..self.n_slots]);
    }

    /// `findWith` minus the DFA guard and the backtracker: always the Pike
    /// VM. Semantically identical — exists so differential tests and
    /// micro-benches can pit the engines against each other.
    pub fn findWithVm(self: *const Regex, scratch: *Scratch, input: []const u8, start: usize, out_slots: []?usize) Error!bool {
        return self.findWithImpl(true, scratch, input, start, out_slots);
    }

    /// Leftmost-first search by depth-first backtracking. The first path to
    /// reach a match is the highest-priority one, so the result equals the
    /// Pike VM's without its per-byte thread lists and capture copies.
    ///
    /// It first runs without a memo under a work budget linear in the
    /// window, which most values finish well inside with no visited set to
    /// clear or consult. Past the budget it reruns exploring each (pc,
    /// position) pair at most once: a pair reached again has already
    /// failed, and start positions share the visited set for the same
    /// reason. Null when that visited set would exceed `BACKTRACK_MAX_BITS`;
    /// the caller then runs the Pike VM.
    fn findBacktrack(self: *const Regex, scratch: *Scratch, input: []const u8, start: usize, out_slots: []?usize) Error!?bool {
        if (self.anchored_start and start > 0 and !self.multiline) return false;
        if (input.len > std.math.maxInt(u32)) return null;
        const width = input.len - start + 1;
        if (scratch.bt_caps.len < self.n_slots) {
            scratch.bt_caps = try scratch.backing.realloc(scratch.bt_caps, self.n_slots);
        }
        const caps = scratch.bt_caps[0..self.n_slots];

        if (scratch.tryUnmemoized(self.prog)) {
            @memset(caps, null);
            var budget = 4 * width + 2 * self.prog.len;
            if (try self.backtrackStarts(false, scratch, input, start, width, &budget, out_slots)) |found| return found;
            scratch.bt_overruns += 1;
        }

        const n_bits = @as(usize, self.bt.rows) * width;
        if (n_bits > BACKTRACK_MAX_BITS) return null;
        const n_words = (n_bits + 63) / 64;
        if (scratch.bt_visited.len < n_words) {
            const grown = @min(BACKTRACK_MAX_BITS / 64, @max(n_words, 2 * scratch.bt_visited.len));
            scratch.backing.free(scratch.bt_visited);
            scratch.bt_visited = &.{};
            scratch.bt_visited = try scratch.backing.alloc(u64, grown);
        }
        @memset(scratch.bt_visited[0..n_words], 0);
        @memset(caps, null);
        var unbounded: usize = 0;
        return try self.backtrackStarts(true, scratch, input, start, width, &unbounded, out_slots);
    }

    /// Null when the search without a memo ran out of `budget`.
    fn backtrackStarts(self: *const Regex, comptime memo: bool, scratch: *Scratch, input: []const u8, start: usize, width: usize, budget: *usize, out_slots: []?usize) Error!?bool {
        var sp = start;
        while (sp <= input.len) : (sp += 1) {
            if (self.anchored_start and sp > 0 and !(self.multiline and input[sp - 1] == '\n')) {
                if (!self.multiline) return false;
                const nl = std.mem.indexOfScalarPos(u8, input, sp, '\n') orelse return false;
                sp = nl + 1;
            } else if (self.prefilter_bytes.len > 0 and sp < input.len) {
                sp = self.nextCandidate(input, sp);
            }
            switch (try self.backtrackFrom(memo, scratch, input, start, width, sp, budget, out_slots)) {
                .matched => return true,
                .failed => {},
                .over_budget => return null,
            }
        }
        return false;
    }

    const BtOutcome = enum { matched, failed, over_budget };

    fn backtrackFrom(self: *const Regex, comptime memo: bool, scratch: *Scratch, input: []const u8, base: usize, width: usize, sp: usize, budget: *usize, out_slots: []?usize) Error!BtOutcome {
        const prog = self.prog;
        const steps = self.bt.steps;
        const visited = scratch.bt_visited;
        const caps = scratch.bt_caps[0..self.n_slots];
        var work = budget.*;
        defer budget.* = work;
        var jobs: JobStack = .{ .buf = scratch.bt_jobs.allocatedSlice() };
        // Pending explore and star_tail jobs. While there are none a save
        // needs no restore: failing back past it ends this start position,
        // whose captures are then cleared for the next.
        var alts: usize = 1;
        var unrestored = false;
        // An anchored program opens with `save 0; ^`, and `backtrackStarts`
        // starts it only where `^` holds, so the search enters past both.
        var entry: u32 = 0;
        if (self.anchored_start) {
            entry = 2;
            caps[0] = @intCast(sp);
            unrestored = true;
        }
        try jobs.push(scratch, .{ .explore = .{ .pc = entry, .pos = @intCast(sp) } });
        while (jobs.len > 0) {
            var pc: u32 = undefined;
            var pos: usize = undefined;
            switch (jobs.buf[jobs.len - 1]) {
                .restore => |r| {
                    caps[r.slot] = r.old;
                    jobs.len -= 1;
                    continue;
                },
                .explore => |e| {
                    pc = e.pc;
                    pos = e.pos;
                    jobs.len -= 1;
                    alts -= 1;
                },
                .star_tail => |*t| {
                    pc = t.pc;
                    pos = t.hi;
                    if (t.hi > t.lo) {
                        t.hi -= 1;
                    } else {
                        jobs.len -= 1;
                        alts -= 1;
                    }
                },
            }
            while (true) {
                if (!memo) {
                    if (work == 0) return .over_budget;
                    work -= 1;
                }
                const step = &steps[pc];
                if (memo) if (step.row) |row| {
                    const bit = @as(usize, row) * width + (pos - base);
                    const mask = @as(u64, 1) << @intCast(bit & 63);
                    if (visited[bit >> 6] & mask != 0) break;
                    visited[bit >> 6] |= mask;
                };
                switch (prog[pc]) {
                    .class => |*cc| {
                        const n = step.lit_len;
                        if (n > 1) {
                            if (input.len - pos < n or !bytesEqual(input[pos..][0..n], self.bt.lit_bytes[pc..][0..n])) break;
                            pc += n;
                            pos += n;
                        } else {
                            if (pos == input.len or !cc.contains(input[pos])) break;
                            pc += 1;
                            pos += 1;
                        }
                    },
                    .match => {
                        copyCaps(out_slots[0..self.n_slots], caps);
                        return .matched;
                    },
                    .jmp => |t| pc = t,
                    .split => |s| switch (step.star) {
                        .none => {
                            const doomed = if (step.alt_guard) |g|
                                pos < input.len and !self.bt.guards[g].contains(input[pos])
                            else
                                false;
                            if (!doomed) {
                                try jobs.push(scratch, .{ .explore = .{ .pc = s.b, .pos = @intCast(pos) } });
                                alts += 1;
                            }
                            pc = s.a;
                        },
                        else => |scan| {
                            // Cross the run of class bytes in one scan; the
                            // continuation is then tried from the run's far end
                            // back to `pos`. With the memo, the loop head's pairs
                            // over the run are marked visited as the byte-by-byte
                            // walk would, and the run stops early at a pair an
                            // earlier path already explored. A star's split is
                            // its jmp's target, so it has a row.
                            const end = starRunEnd(scan, &prog[pc + 1].class, input, pos);
                            var top = end;
                            if (memo and end > pos) {
                                const bit = @as(usize, step.row.?) * width + (pos - base);
                                if (bitsFirstSet(visited, bit + 1, bit + (end - pos) + 1)) |hit| top = pos + (hit - bit) - 1;
                                bitsSetRange(visited, bit + 1, bit + (top - pos) + 1);
                            }
                            if (!memo) work -|= end - pos;
                            if (step.star_tail and top > pos) {
                                try jobs.push(scratch, .{ .star_tail = .{ .pc = s.b, .lo = @intCast(pos), .hi = @intCast(top - 1) } });
                                alts += 1;
                            }
                            pc = s.b;
                            pos = top;
                        },
                    },
                    .save => |slot| {
                        // `save 1` is the program's last step before `match`.
                        if (slot == 1) {
                            caps[1] = @intCast(pos);
                            copyCaps(out_slots[0..self.n_slots], caps);
                            return .matched;
                        }
                        if (alts > 0) {
                            try jobs.push(scratch, .{ .restore = .{ .slot = slot, .old = caps[slot] } });
                        } else {
                            unrestored = true;
                        }
                        caps[slot] = @intCast(pos);
                        pc += 1;
                    },
                    .bol => {
                        if (pos != 0 and !(self.multiline and input[pos - 1] == '\n')) break;
                        pc += 1;
                    },
                    .eol => {
                        if (pos != input.len and !(self.multiline and input[pos] == '\n')) break;
                        pc += 1;
                    },
                    .word_boundary, .not_word_boundary => {
                        const before = pos > 0 and isWordByte(input[pos - 1]);
                        const after = pos < input.len and isWordByte(input[pos]);
                        if ((before != after) != (prog[pc] == .word_boundary)) break;
                        pc += 1;
                    },
                }
            }
        }
        if (unrestored) @memset(caps, null);
        return .failed;
    }

    /// DFA-side of `matchesWith`: null when the DFA can't answer (pattern
    /// has word boundaries, too many distinct byte classes, or the state
    /// cache overflowed) — the caller must use the VM.
    noinline fn dfaMatches(self: *const Regex, scratch: *Scratch, input: []const u8, start: usize) Error!?bool {
        if (self.has_wordbound) return null;
        if (scratch.dfa == null or scratch.dfa_prog != @intFromPtr(self.prog.ptr)) {
            if (scratch.dfa) |*d| d.deinit();
            scratch.dfa = null;
            scratch.dfa = try Dfa.init(scratch.backing, self.prog, self.multiline);
            scratch.dfa_prog = @intFromPtr(self.prog.ptr);
        }
        return scratch.dfa.?.matches(input, start);
    }

    /// `enable_skip` toggles the stationary-run bulk-skip fast path. The
    /// `false` instantiation is the reference matcher used by the
    /// differential property test to validate the `true` one; it is dead
    /// code in any non-test build.
    noinline fn findWithImpl(self: *const Regex, comptime enable_skip: bool, scratch: *Scratch, input: []const u8, start: usize, out_slots: []?usize) Error!bool {
        try scratch.prepare(self.prog.len, self.n_slots, input.len);
        _ = scratch.arena.reset(.retain_capacity);

        const VM = struct {
            prog: []const Inst,
            input: []const u8,
            multiline: bool,
            caps_alloc: Allocator,
            list_alloc: Allocator,
            visited: []u32,
            gen: u32,

            const List = std.ArrayListUnmanaged(Thread);

            fn addThread(vm: *@This(), list: *List, pc: u32, pos: usize, caps: []?usize) Error!void {
                if (vm.visited[pc] == vm.gen) return;
                vm.visited[pc] = vm.gen;
                switch (vm.prog[pc]) {
                    .jmp => |t| try vm.addThread(list, t, pos, caps),
                    .split => |s| {
                        try vm.addThread(list, s.a, pos, caps);
                        try vm.addThread(list, s.b, pos, caps);
                    },
                    .save => |slot| {
                        const caps2 = try vm.caps_alloc.dupe(?usize, caps);
                        caps2[slot] = pos;
                        try vm.addThread(list, pc + 1, pos, caps2);
                    },
                    .bol => if (pos == 0 or (vm.multiline and vm.input[pos - 1] == '\n')) try vm.addThread(list, pc + 1, pos, caps),
                    .eol => if (pos == vm.input.len or (vm.multiline and vm.input[pos] == '\n')) try vm.addThread(list, pc + 1, pos, caps),
                    .word_boundary, .not_word_boundary => {
                        const before = pos > 0 and isWordByte(vm.input[pos - 1]);
                        const after = pos < vm.input.len and isWordByte(vm.input[pos]);
                        const at_boundary = before != after;
                        const want = vm.prog[pc] == .word_boundary;
                        if (at_boundary == want) try vm.addThread(list, pc + 1, pos, caps);
                    },
                    .class, .match => try list.append(vm.list_alloc, .{ .pc = pc, .caps = caps }),
                }
            }
        };

        var vm = VM{
            .prog = self.prog,
            .input = input,
            .multiline = self.multiline,
            .caps_alloc = scratch.arena.allocator(),
            .list_alloc = scratch.backing,
            .visited = scratch.visited,
            .gen = scratch.gen,
        };

        // `clist` = epsilon-closed threads ready to consume a byte (only
        // `.class`/`.match` instructions). `carried` = the raw `.class`
        // targets advanced from the previous step, closed into `clist` at
        // the top of each step under a fresh dedup generation. Both lists
        // live in the scratch and are reused across rows; per-thread capture
        // arrays come from the scratch arena.
        const clist = &scratch.clist;
        const carried = &scratch.carried;
        clist.clearRetainingCapacity();
        carried.clearRetainingCapacity();
        var matched: ?[]?usize = null;

        const seed = scratch.seed[0..self.n_slots];

        var sp = start;
        while (true) : (sp += 1) {
            // Unanchored first-byte prefilter: with no match yet and no
            // in-progress thread carried from the previous step, a match can
            // only begin at a byte in `prefilter_bytes`, so memchr straight to
            // the next candidate instead of seeding + stepping dead positions.
            if (enable_skip and matched == null and carried.items.len == 0 and
                self.prefilter_bytes.len > 0 and sp < input.len)
            {
                sp = self.nextCandidate(input, sp);
            }

            vm.gen += 1;
            clist.clearRetainingCapacity();
            // Carried threads (earlier-starting) get higher priority.
            for (carried.items) |t| try vm.addThread(clist, t.pc, sp, t.caps);
            // Seed an unanchored start thread at the lowest priority until a
            // match begins, so an earlier-starting match wins (leftmost). For
            // a `^`-anchored program the seed can only survive where `.bol`
            // holds, so skip it everywhere else — this removes the
            // per-position seed work that otherwise dominates anchored scans.
            if (matched == null and (!self.anchored_start or sp == 0 or (self.multiline and input[sp - 1] == '\n'))) {
                try vm.addThread(clist, 0, sp, seed);
            }
            carried.clearRetainingCapacity();

            const c: ?u8 = if (sp < input.len) input[sp] else null;
            var i: usize = 0;
            while (i < clist.items.len) : (i += 1) {
                const t = clist.items[i];
                switch (self.prog[t.pc]) {
                    .class => |cc| if (c) |ch| {
                        if (cc.contains(ch)) try carried.append(vm.list_alloc, .{ .pc = t.pc + 1, .caps = t.caps });
                    },
                    .match => {
                        // Highest-priority match wins; lower-priority
                        // threads can't beat it (leftmost-then-greedy).
                        matched = t.caps;
                        break;
                    },
                    else => unreachable, // epsilon insts resolved in addThread
                }
            }

            // ---- stationary class-loop bulk-skip ---------------------------
            // The single step just taken consumed `ch` and produced `carried`.
            // If re-closing `carried` reproduces the exact thread list we
            // started this step with, the loop is stationary: every following
            // byte that survives the same way is an identical step, so the run
            // can be collapsed into one forward scan. This is what turns the
            // `[^/]+` host and `.*$` tail of the ClickBench host-extract from
            // O(len) Pike steps into a near-memchr scan.
            //
            // Guards keeping the collapse exact:
            //   * `enable_skip` and no pending match (a match must be reported
            //     at its true position, not skipped over);
            //   * `^`-anchored program, so no start thread is seeded mid-line
            //     (seeding would change clist between steps); the scan also
            //     stops at any '\n', the only mid-string seed point;
            //   * no `\b`/`\B` (their closure is position-dependent);
            //   * `ch != '\n'` so the probe position (sp+1) sees the same
            //     anchor state as the run interior;
            //   * the probe (clist at sp+1) equals the current clist as an
            //     ordered list, with survivors keeping their capture pointer
            //     (a `.save` on the loop path changes it → not stationary),
            //     and introduces no `.match`.
            if (enable_skip and matched == null and self.anchored_start and
                !self.has_wordbound and clist.items.len > 0 and
                clist.items.len <= 64 and carried.items.len > 0)
            {
                const ch = c.?;
                if (ch != '\n') skip: {
                    var sig0: u64 = 0;
                    var any_loop = false;
                    for (clist.items, 0..) |t, k| {
                        if (self.prog[t.pc].class.contains(ch)) {
                            sig0 |= (@as(u64, 1) << @intCast(k));
                            if (self.loopy[t.pc]) any_loop = true;
                        }
                    }
                    // Only a class in a `*`/`+` body can sustain a stationary
                    // run; bail before the probe otherwise (e.g. literal prefix).
                    if (!any_loop) break :skip;

                    vm.gen += 1;
                    scratch.probe.clearRetainingCapacity();
                    for (carried.items) |t| try vm.addThread(&scratch.probe, t.pc, sp + 1, t.caps);
                    if (scratch.probe.items.len != clist.items.len) break :skip;
                    for (scratch.probe.items, 0..) |pt, k| {
                        const ct = clist.items[k];
                        if (pt.pc != ct.pc) break :skip;
                        if (self.prog[pt.pc] == .match) break :skip;
                        const survivor = (sig0 & (@as(u64, 1) << @intCast(k))) != 0;
                        if (survivor and pt.caps.ptr != ct.caps.ptr) break :skip;
                    }

                    sp = self.runEnd(clist.items, sig0, input, sp + 1) - 1; // outer `sp += 1` resumes at the run end
                }
            }

            if (c == null) break;
            if (matched != null and carried.items.len == 0) break;
        }
        scratch.gen = vm.gen;

        if (matched) |m| {
            @memcpy(out_slots[0..self.n_slots], m);
            return true;
        }
        return false;
    }

    /// First index `>= from` at which a stationary class-loop run ends: the
    /// first byte whose surviving-class signature differs from `sig0`, or a
    /// '\n' (a mid-string `^` seed point), or end-of-input.
    ///
    /// The set of bytes that *continue* the run is, exactly,
    /// `(∩ survivor classes) ∩ (∩ complement of non-survivor classes)` minus
    /// '\n' — computed once here by bitwise ops over the `CharClass` masks
    /// rather than re-deriving the per-thread signature for every byte. When
    /// the run has only a few distinct breaker bytes (the usual case: `[^/]+`
    /// breaks on '/', `.*` on '\n'), each is found with a vectorized
    /// `indexOfScalar` (memchr) and the earliest wins.
    fn runEnd(self: *const Regex, clist_items: []const Thread, sig0: u64, input: []const u8, from: usize) usize {
        var cont: CharClass = .{ .bits = [_]u8{0xFF} ** 32 };
        for (clist_items, 0..) |t, k| {
            const cc = self.prog[t.pc].class;
            if ((sig0 >> @intCast(k)) & 1 != 0) {
                for (&cont.bits, cc.bits) |*x, y| x.* &= y;
            } else {
                for (&cont.bits, cc.bits) |*x, y| x.* &= ~y;
            }
        }
        // '\n' ends the run even when a survivor class would accept it.
        cont.bits['\n' >> 3] &= ~(@as(u8, 1) << ('\n' & 7));

        // Breaker bytes = complement of the continue set. If there are only a
        // handful, memchr to the first; otherwise fall back to a tight scan
        // over the precomputed set.
        var breakers: [4]u8 = undefined;
        var nb: usize = 0;
        for (cont.bits, 0..) |m, bi| {
            const inv = ~m;
            if (inv == 0) continue;
            var bit: u8 = 0;
            while (bit < 8) : (bit += 1) {
                if ((inv >> @intCast(bit)) & 1 != 0) {
                    if (nb == breakers.len) {
                        // Dense breaker set: tight scalar membership scan. A
                        // SIMD range scan was tried here and measured neutral —
                        // the per-run range detection cancels its benefit at
                        // typical run lengths.
                        var j = from;
                        while (j < input.len and cont.contains(input[j])) : (j += 1) {}
                        return j;
                    }
                    breakers[nb] = @intCast(bi * 8 + bit);
                    nb += 1;
                }
            }
        }
        var best = input.len;
        for (breakers[0..nb]) |x| {
            if (std.mem.indexOfScalarPos(u8, input, from, x)) |idx| {
                if (idx < best) best = idx;
            }
        }
        return best;
    }

    /// First index `>= from` whose byte is a possible match start (in
    /// `prefilter_bytes`), or `input.len`.
    fn nextCandidate(self: *const Regex, input: []const u8, from: usize) usize {
        return findAnyByte(input, from, self.prefilter_bytes);
    }

    /// Replace every non-overlapping match in `input`. `template` may
    /// reference capture groups with `\0`..`\9` (`\0` = whole match).
    /// Caller owns the returned slice.
    pub fn replaceAll(self: *const Regex, allocator: Allocator, input: []const u8, template: []const u8) Error![]u8 {
        var scratch = Scratch.init(allocator);
        defer scratch.deinit();
        const borrowed = try self.replaceAllScratch(input, template, &scratch);
        return allocator.dupe(u8, borrowed);
    }

    /// Like `replaceAll`, but the caller supplies a reusable `Scratch` and
    /// the result is *borrowed* from `scratch.out_buf` (valid only until the
    /// next call sharing this scratch). Reusing the scratch across the rows
    /// of a batch avoids per-row allocation — the dominant fixed cost when
    /// applying one compiled pattern to millions of values.
    pub fn replaceAllScratch(
        self: *const Regex,
        input: []const u8,
        template: []const u8,
        scratch: *Scratch,
    ) Error![]const u8 {
        return self.replaceScratch(input, template, scratch, 0, 0);
    }

    /// `replaceAllScratch` from byte `start` on, keeping the text before
    /// it; a nonzero `occurrence` replaces only that match (1-based) and
    /// leaves the others. An empty match where the previous match ended is
    /// no match, as in RE2's GlobalReplace, PostgreSQL and DuckDB: `.*`
    /// replaces `abcd` once, not again at its end.
    pub fn replaceScratch(
        self: *const Regex,
        input: []const u8,
        template: []const u8,
        scratch: *Scratch,
        start: usize,
        occurrence: usize,
    ) Error![]const u8 {
        const allocator = scratch.backing;
        if (scratch.slots.len < self.n_slots) {
            scratch.slots = try scratch.backing.realloc(scratch.slots, self.n_slots);
        }
        const slots = scratch.slots[0..self.n_slots];

        const out = &scratch.out_buf;
        out.clearRetainingCapacity();
        try out.appendSlice(allocator, input[0..start]);

        var pos: usize = start;
        var seen: usize = 0;
        var last_end: ?usize = null;
        while (pos <= input.len) {
            const found = try self.findWith(scratch, input, pos, slots);
            if (!found) break;
            const m_start = slots[0].?;
            const m_end = slots[1].?;
            try out.appendSlice(allocator, input[pos..m_start]);
            if (m_end == m_start and last_end == m_start) {
                pos = afterMatch(input, m_start, m_end);
                try out.appendSlice(allocator, input[m_end..@min(pos, input.len)]);
                continue;
            }
            last_end = m_end;
            seen += 1;
            if (occurrence == 0 or seen == occurrence) {
                try expandTemplate(allocator, out, template, input, slots);
            } else {
                try out.appendSlice(allocator, input[m_start..m_end]);
            }
            pos = afterMatch(input, m_start, m_end);
            // An empty match consumes nothing, so the character it sits
            // before is copied here, not left for the next match to skip.
            if (m_end == m_start) try out.appendSlice(allocator, input[m_end..@min(pos, input.len)]);
            // An anchored pattern matches only at the text's start.
            if (seen == occurrence or (self.anchored_start and !self.multiline)) break;
        }
        if (pos < input.len) try out.appendSlice(allocator, input[pos..]);
        return out.items;
    }

    /// The `occurrence`-th (1-based) match at or after byte `start`, each
    /// search resuming where the previous match ended. False when there are
    /// fewer matches.
    pub fn findNth(self: *const Regex, scratch: *Scratch, input: []const u8, start: usize, occurrence: usize, out_slots: []?usize) Error!bool {
        var pos = start;
        var seen: usize = 0;
        while (pos <= input.len) {
            if (!try self.findWith(scratch, input, pos, out_slots)) return false;
            seen += 1;
            if (seen >= occurrence) return true;
            pos = afterMatch(input, out_slots[0].?, out_slots[1].?);
        }
        return false;
    }
};

/// Where the search after a match resumes: its end, or past the whole
/// UTF-8 character after an empty match, so the next search can't find
/// the same empty match or start inside a character.
pub fn afterMatch(input: []const u8, m_start: usize, m_end: usize) usize {
    if (m_end > m_start) return m_end;
    var next = m_end + 1;
    while (next < input.len and input[next] & 0xC0 == 0x80) next += 1;
    return next;
}

// ---------------------------------------------------------------------------
// Lazy DFA — exact match-existence in one linear pass
// ---------------------------------------------------------------------------
//
// Subset construction over the compiled program, built lazily: a DFA state is
// the (sorted, deduped) set of instruction pointers alive after consuming the
// previous byte, plus a flag that resolves `.bol`: at the text's start, or,
// multiline, just after a '\n'.
// Transitions are built on first use per (state, byte-class); `.eol` resolves
// against the byte being consumed (or EOF) and `.save` is plain epsilon. The
// DFA recognizes exactly the language the Pike VM does — it answers "does a
// match exist at or after `start`" with no priority or capture semantics
// involved, so the answer is exact while costing one table lookup per byte.
// The seed (pc 0) joins every closure, which is what "match may start at any
// position" means; for a `^`-anchored program the seed's `.bol` kills it
// mid-line, so anchoring needs no special casing.
//
// Self-loop runs collapse the way the VM's stationary-run bulk-skip does:
// when the matcher observes a state transitioning to itself, it completes
// that state's transition row, derives the byte set that LEAVES the state,
// and crosses the run with memchr (few breaker bytes) or one membership scan
// (dense).
//
// Word boundaries are declined (their closure would need prev-byte word-ness
// in the state key — not worth doubling the state space), as are patterns
// with more than 63 distinct class bitmaps; the state cache is capped. Any
// of these marks the DFA failed and `matches` returns null — the caller
// falls back to the Pike VM, so behavior is unchanged, only slower.

const DfaId = u16;
const DFA_UNSET: DfaId = std.math.maxInt(u16);
const DFA_ACCEPT: DfaId = std.math.maxInt(u16) - 1;
const DFA_MAX_STATES: usize = 224;

const DfaState = struct {
    pcs_off: u32,
    pcs_len: u32,
    prev_nl: bool,
    eof_built: bool = false,
    eof_accept: bool = false,
    /// Set once the full transition row exists; enables the self-loop skip.
    skip_ready: bool = false,
    /// Bytes that keep the state looping on itself (valid when skip_ready).
    stay: CharClass = .{},
    /// When the breaker set (complement of `stay`) is small, its bytes —
    /// each found with a vectorized memchr. 0xFF in n_break means "dense:
    /// scan with stay.contains instead".
    n_break: u8 = 0,
    break_bytes: [4]u8 = .{ 0, 0, 0, 0 },
};

pub const Dfa = struct {
    allocator: Allocator,
    prog: []const Inst,
    n_classes: u16 = 0,
    class_of: [256]u8 = undefined,
    class_rep: [256]u8 = undefined,
    states: std.ArrayListUnmanaged(DfaState) = .empty,
    pc_pool: std.ArrayListUnmanaged(u32) = .empty,
    trans: std.ArrayListUnmanaged(DfaId) = .empty,
    map: std.AutoHashMapUnmanaged(u64, DfaId) = .empty,
    visited: []u32,
    gen: u32 = 0,
    stack: std.ArrayListUnmanaged(u32) = .empty,
    next_pcs: std.ArrayListUnmanaged(u32) = .empty,
    failed: bool = false,
    multiline: bool,

    fn init(backing: Allocator, prog: []const Inst, multiline: bool) Error!Dfa {
        var d = Dfa{
            .allocator = backing,
            .prog = prog,
            .multiline = multiline,
            .visited = try backing.alloc(u32, prog.len),
        };
        @memset(d.visited, 0);

        // Distinct class bitmaps drive the byte-class partition: two bytes
        // behave identically iff they agree on every bitmap and on '\n'-ness.
        var maps: [63]*const CharClass = undefined;
        var n_maps: usize = 0;
        for (prog) |*inst| {
            if (inst.* != .class) continue;
            const cc = &inst.class;
            var found = false;
            for (maps[0..n_maps]) |m| {
                if (std.mem.eql(u8, &m.bits, &cc.bits)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                if (n_maps == maps.len) {
                    d.failed = true;
                    return d;
                }
                maps[n_maps] = cc;
                n_maps += 1;
            }
        }
        var sigs: [256]u64 = undefined;
        var n_classes: usize = 0;
        for (0..256) |b| {
            var sig: u64 = if (b == '\n') 1 else 0;
            for (maps[0..n_maps], 0..) |m, mi| {
                if (m.contains(@intCast(b))) sig |= @as(u64, 1) << @intCast(mi + 1);
            }
            var cid: usize = n_classes;
            for (sigs[0..n_classes], 0..) |s, i| {
                if (s == sig) {
                    cid = i;
                    break;
                }
            }
            if (cid == n_classes) {
                sigs[n_classes] = sig;
                d.class_rep[n_classes] = @intCast(b);
                n_classes += 1;
            }
            d.class_of[b] = @intCast(cid);
        }
        d.n_classes = @intCast(n_classes);
        return d;
    }

    pub fn deinit(self: *Dfa) void {
        self.allocator.free(self.visited);
        self.states.deinit(self.allocator);
        self.pc_pool.deinit(self.allocator);
        self.trans.deinit(self.allocator);
        self.map.deinit(self.allocator);
        self.stack.deinit(self.allocator);
        self.next_pcs.deinit(self.allocator);
    }

    /// Exact match-existence at or after `start`; null = DFA unavailable
    /// (caller must use the VM).
    pub fn matches(self: *Dfa, input: []const u8, start: usize) Error!?bool {
        if (self.failed) return null;
        const prev_nl = start == 0 or (self.multiline and input[start - 1] == '\n');
        var sid = (try self.stateFor(&.{}, prev_nl)) orelse return null;
        var sp = start;
        while (sp < input.len) {
            const b = input[sp];
            const cls: usize = self.class_of[b];
            var t = self.trans.items[@as(usize, sid) * self.n_classes + cls];
            if (t == DFA_UNSET) {
                t = (try self.buildTrans(sid, cls, b)) orelse return null;
            }
            if (t == DFA_ACCEPT) return true;
            if (t == sid) {
                if (!self.states.items[sid].skip_ready) {
                    if (!try self.completeRow(sid)) return null;
                }
                sp = self.runSkip(sid, input, sp + 1);
                continue;
            }
            sid = t;
            sp += 1;
        }
        return try self.eofAccept(sid);
    }

    fn buildTrans(self: *Dfa, sid: DfaId, cls: usize, b: u8) Error!?DfaId {
        const t = (try self.computeTransition(sid, b)) orelse return null;
        self.trans.items[@as(usize, sid) * self.n_classes + cls] = t;
        return t;
    }

    /// Epsilon-close the state's pcs (plus the position seed) against byte
    /// `b` (null = EOF), collecting the surviving class successors into
    /// `next_pcs`. Returns true when the closure reaches `.match`.
    fn closureWalk(self: *Dfa, sid: DfaId, b: ?u8) Error!bool {
        if (self.gen == std.math.maxInt(u32)) {
            @memset(self.visited, 0);
            self.gen = 0;
        }
        self.gen += 1;
        self.stack.clearRetainingCapacity();
        self.next_pcs.clearRetainingCapacity();
        const st = self.states.items[sid];
        const pcs = self.pc_pool.items[st.pcs_off .. st.pcs_off + st.pcs_len];
        // Seed last so it pops first — order is irrelevant for existence,
        // but keeping it deterministic keeps states canonical.
        for (pcs) |pc| try self.stack.append(self.allocator, pc);
        try self.stack.append(self.allocator, 0);
        var accept = false;
        while (self.stack.items.len > 0) {
            const pc = self.stack.items[self.stack.items.len - 1];
            self.stack.items.len -= 1;
            if (self.visited[pc] == self.gen) continue;
            self.visited[pc] = self.gen;
            switch (self.prog[pc]) {
                .jmp => |tgt| try self.stack.append(self.allocator, tgt),
                .split => |s| {
                    try self.stack.append(self.allocator, s.a);
                    try self.stack.append(self.allocator, s.b);
                },
                .save => try self.stack.append(self.allocator, pc + 1),
                .bol => if (st.prev_nl) try self.stack.append(self.allocator, pc + 1),
                .eol => if (b == null or (self.multiline and b.? == '\n')) try self.stack.append(self.allocator, pc + 1),
                // Never compiled into a DFA-eligible program (dfaMatches
                // declines has_wordbound patterns before building one).
                .word_boundary, .not_word_boundary => unreachable,
                .class => |cc| if (b) |ch| {
                    if (cc.contains(ch)) try self.next_pcs.append(self.allocator, pc + 1);
                },
                .match => accept = true,
            }
        }
        return accept;
    }

    fn computeTransition(self: *Dfa, sid: DfaId, b: u8) Error!?DfaId {
        if (try self.closureWalk(sid, b)) return DFA_ACCEPT;
        std.mem.sort(u32, self.next_pcs.items, {}, std.sort.asc(u32));
        return self.stateFor(self.next_pcs.items, self.multiline and b == '\n');
    }

    fn stateFor(self: *Dfa, pcs: []const u32, prev_nl: bool) Error!?DfaId {
        var h = std.hash.Wyhash.init(0x5EED_DFA1);
        h.update(&[1]u8{@intFromBool(prev_nl)});
        h.update(std.mem.sliceAsBytes(pcs));
        const key = h.final();
        if (self.map.get(key)) |id| {
            // Verify against a (vanishingly unlikely) 64-bit hash collision;
            // sound either way — a mismatch just retires the DFA.
            const st = self.states.items[id];
            const stored = self.pc_pool.items[st.pcs_off .. st.pcs_off + st.pcs_len];
            if (st.prev_nl == prev_nl and std.mem.eql(u32, stored, pcs)) return id;
            self.failed = true;
            return null;
        }
        if (self.states.items.len >= DFA_MAX_STATES) {
            self.failed = true;
            return null;
        }
        const off: u32 = @intCast(self.pc_pool.items.len);
        try self.pc_pool.appendSlice(self.allocator, pcs);
        const id: DfaId = @intCast(self.states.items.len);
        try self.states.append(self.allocator, .{
            .pcs_off = off,
            .pcs_len = @intCast(pcs.len),
            .prev_nl = prev_nl,
        });
        try self.trans.appendNTimes(self.allocator, DFA_UNSET, self.n_classes);
        try self.map.put(self.allocator, key, id);
        return id;
    }

    /// Build every remaining transition of `sid`'s row, then derive the
    /// self-loop skip info: the byte set that keeps the state looping and
    /// the (small) breaker set that ends a run.
    fn completeRow(self: *Dfa, sid: DfaId) Error!bool {
        var c: usize = 0;
        while (c < self.n_classes) : (c += 1) {
            const idx = @as(usize, sid) * self.n_classes + c;
            if (self.trans.items[idx] != DFA_UNSET) continue;
            if ((try self.buildTrans(sid, c, self.class_rep[c])) == null) return false;
        }
        var stay = CharClass{};
        var n_break: usize = 0;
        var break_bytes: [4]u8 = .{ 0, 0, 0, 0 };
        for (0..256) |b| {
            if (self.trans.items[@as(usize, sid) * self.n_classes + self.class_of[b]] == sid) {
                stay.add(@intCast(b));
            } else {
                if (n_break < break_bytes.len) break_bytes[n_break] = @intCast(b);
                n_break += 1;
            }
        }
        const st = &self.states.items[sid];
        st.stay = stay;
        st.n_break = if (n_break <= break_bytes.len) @intCast(n_break) else 0xFF;
        st.break_bytes = break_bytes;
        st.skip_ready = true;
        return true;
    }

    /// First index `>= from` whose byte leaves the self-looping state, or
    /// input.len. Mirrors the VM bulk-skip's runEnd: memchr per breaker byte
    /// when the breaker set is small, membership scan otherwise.
    fn runSkip(self: *const Dfa, sid: DfaId, input: []const u8, from: usize) usize {
        const st = self.states.items[sid];
        if (st.n_break != 0xFF) {
            var best = input.len;
            for (st.break_bytes[0..st.n_break]) |x| {
                if (std.mem.indexOfScalarPos(u8, input, from, x)) |idx| {
                    if (idx < best) best = idx;
                }
            }
            return best;
        }
        var j = from;
        while (j < input.len and st.stay.contains(input[j])) : (j += 1) {}
        return j;
    }

    fn eofAccept(self: *Dfa, sid: DfaId) Error!bool {
        if (!self.states.items[sid].eof_built) {
            const accept = try self.closureWalk(sid, null);
            const st = &self.states.items[sid];
            st.eof_accept = accept;
            st.eof_built = true;
        }
        return self.states.items[sid].eof_accept;
    }
};

fn expandTemplate(allocator: Allocator, out: *std.ArrayList(u8), template: []const u8, input: []const u8, slots: []const ?usize) Error!void {
    var i: usize = 0;
    while (i < template.len) : (i += 1) {
        const c = template[i];
        if ((c == '\\' or c == '$') and i + 1 < template.len and template[i + 1] >= '0' and template[i + 1] <= '9') {
            const g: usize = template[i + 1] - '0';
            i += 1;
            const lo_i = 2 * g;
            const hi_i = 2 * g + 1;
            if (hi_i < slots.len) {
                if (slots[lo_i]) |s| if (slots[hi_i]) |e| {
                    try out.appendSlice(allocator, input[s..e]);
                };
            }
        } else if (c == '\\' and i + 1 < template.len) {
            // Escaped literal in the template.
            i += 1;
            try out.append(allocator, template[i]);
        } else {
            try out.append(allocator, c);
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

fn expectReplace(pattern: []const u8, input: []const u8, template: []const u8, expected: []const u8) !void {
    var re = try Regex.compile(std.testing.allocator, pattern);
    defer re.deinit();
    const got = try re.replaceAll(std.testing.allocator, input, template);
    defer std.testing.allocator.free(got);
    try std.testing.expectEqualStrings(expected, got);
}

fn expectMatch(pattern: []const u8, input: []const u8, should_match: bool) !void {
    var re = try Regex.compile(std.testing.allocator, pattern);
    defer re.deinit();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const slots = try std.testing.allocator.alloc(?usize, re.n_slots);
    defer std.testing.allocator.free(slots);
    const found = try re.find(arena.allocator(), input, 0, slots);
    try std.testing.expectEqual(should_match, found);
}

test "regex: literal + dot + quantifiers" {
    try expectMatch("abc", "xabcy", true);
    try expectMatch("a.c", "abc", true);
    try expectMatch("a.c", "ac", false);
    try expectMatch("ab*c", "ac", true);
    try expectMatch("ab+c", "ac", false);
    try expectMatch("ab+c", "abbbc", true);
    try expectMatch("colou?r", "color", true);
    try expectMatch("colou?r", "colour", true);
}

test "regex: classes + shorthands + anchors" {
    try expectMatch("[a-c]+", "bbb", true);
    try expectMatch("[^/]+", "abc", true);
    try expectMatch("^\\d{3}$", "123", true);
    try expectMatch("^\\d{3}$", "12", false);
    try expectMatch("^\\d{2,4}$", "12345", false);
    try expectMatch("\\w+@\\w+", "a@b", true);
    try expectMatch("\\bword\\b", "a word here", true);
    try expectMatch("\\bword\\b", "awordhere", false);
}

test "regex: alternation + groups" {
    try expectMatch("(cat|dog)s?", "dogs", true);
    try expectMatch("(?:ab)+", "ababab", true);
}

test "regex: replaceAll with capture backrefs" {
    try expectReplace("a", "banana", "X", "bXnXnX");
    try expectReplace("(\\w+)@(\\w+)", "x@y", "\\2.\\1", "y.x");
    // ClickBench Q28 pattern: extract hostname from a URL.
    try expectReplace(
        "^https?://(?:www\\.)?([^/]+)/.*$",
        "https://www.example.com/path/page",
        "\\1",
        "example.com",
    );
    try expectReplace(
        "^https?://(?:www\\.)?([^/]+)/.*$",
        "http://sub.host.org/x",
        "\\1",
        "sub.host.org",
    );
    // No match → input unchanged.
    try expectReplace("^https?://([^/]+)/.*$", "not a url", "\\1", "not a url");
}

test "regex: lazy quantifier" {
    var re = try Regex.compile(std.testing.allocator, "<.*?>");
    defer re.deinit();
    const got = try re.replaceAll(std.testing.allocator, "<a><b>", "X");
    defer std.testing.allocator.free(got);
    try std.testing.expectEqualStrings("XX", got);
}

test "regex: invalid patterns rejected" {
    try std.testing.expectError(Error.RegexInvalidPattern, Regex.compile(std.testing.allocator, "(abc"));
    try std.testing.expectError(Error.RegexInvalidPattern, Regex.compile(std.testing.allocator, "[abc"));
    try std.testing.expectError(Error.RegexInvalidPattern, Regex.compile(std.testing.allocator, "abc)"));
    // Lookaround is explicitly unsupported (only `(?:` is allowed).
    try std.testing.expectError(Error.RegexInvalidPattern, Regex.compile(std.testing.allocator, "a(?=b)"));
    // So are Unicode property classes, in a class or out of one.
    inline for (.{ "\\p{Han}", "\\pL", "[\\p{Han}]+", "[^\\P{L}]", "[a-\\p]" }) |pattern| {
        try std.testing.expectError(Error.RegexInvalidPattern, Regex.compile(std.testing.allocator, pattern));
    }
}

test "regex: dot does not match newline" {
    try expectMatch("a.b", "axb", true);
    try expectMatch("^a.b$", "a\nb", false);
}

test "regex: shorthand classes incl. negated" {
    try expectMatch("^\\s+$", " \t \n", true);
    try expectMatch("^\\S+$", "abc", true);
    try expectMatch("^\\S+$", "a c", false);
    try expectMatch("^\\D+$", "abc.", true);
    try expectMatch("^\\D+$", "ab1", false);
    try expectMatch("^\\W+$", "...", true);
    try expectMatch("^\\W+$", "a.", false);
}

test "regex: bounded + unbounded repetition" {
    try expectMatch("^a{3}$", "aaa", true);
    try expectMatch("^a{3}$", "aa", false);
    try expectMatch("^a{2,}$", "aa", true);
    try expectMatch("^a{2,}$", "aaaaa", true);
    try expectMatch("^a{2,}$", "a", false);
    try expectMatch("^a{2,3}$", "aaaa", false);
}

test "regex: a brace that starts no repeat is a literal brace, as in RE2" {
    try expectMatch("^a{,2}$", "a{,2}", true);
    try expectMatch("^a{,2}$", "aa", false);
    try expectMatch("^a{,}$", "a{,}", true);
    try expectMatch("^a{x}$", "a{x}", true);
    try expectMatch("^a{2$", "a{2", true);
    try expectMatch("^a{2,x}$", "a{2,x}", true);
    try expectMatch("^{}$", "{}", true);
    try expectMatch("^x{,}{2}$", "x{,}}", true);
    // A real repeat still needs something to repeat.
    try std.testing.expectError(Error.RegexInvalidPattern, Regex.compile(std.testing.allocator, "{2}"));
    try std.testing.expectError(Error.RegexInvalidPattern, Regex.compile(std.testing.allocator, "a|{1,3}"));
}

test "regex: multi-way alternation + nested groups" {
    try expectMatch("^(red|green|blue)$", "green", true);
    try expectMatch("^(red|green|blue)$", "yellow", false);
    // Nested capture indices: outer=1, inner=2.
    try expectReplace("((ab)c)", "abc", "\\2-\\1", "ab-abc");
}

test "regex: word-boundary negation \\B" {
    try expectMatch("\\Bcat\\B", "scatter", true); // 'cat' inside a word
    try expectMatch("\\Bcat\\B", "the cat sat", false); // standalone word
}

test "regex: escaped metacharacters are literal" {
    try expectMatch("^a\\.b$", "a.b", true);
    try expectMatch("^a\\.b$", "axb", false);
    try expectMatch("^\\(\\+\\)$", "(+)", true);
}

test "regex: class edge cases ([]a] literal ], ranges, trailing -)" {
    try expectMatch("^[]a]+$", "]a]a", true); // leading ] is a literal member
    try expectMatch("^[a-]+$", "a-a-", true); // trailing - is literal
    try expectMatch("^[0-9A-Fa-f]+$", "1aF", true);
    try expectMatch("^[0-9A-Fa-f]+$", "1aG", false);
}

test "regex: POSIX classes inside brackets" {
    try expectMatch("^[[:lower:]]+C[[:lower:]]+$", "bCd", true);
    try expectMatch("^[[:lower:]]+$", "bCd", false);
    try expectMatch("^[[:alpha:][:digit:]_]+$", "a1_Z", true);
    try expectMatch("^[[:^digit:]]+$", "ab c", true);
    try expectMatch("^[[:^digit:]]+$", "ab1", false);
    try expectMatch("^[^[:space:]]+$", "a\tb", false);
    try expectMatch("^[[:punct:]]+$", "!?.", true);
    try expectMatch("^[[:xdigit:]]+$", "0fA", true);
    // Without its closing ":]", "[:" is two members.
    try expectMatch("^[[:a]+$", "[:a", true);
    try std.testing.expectError(Error.RegexInvalidPattern, Regex.compile(std.testing.allocator, "[[:nope:]]"));
}

test "regex: greedy vs lazy replace" {
    try expectReplace("a+", "aaa bbb aaa", "X", "X bbb X"); // greedy: whole run
    try expectReplace("a+?", "aaa", "X", "XXX"); // lazy: one at a time
}

test "regex: replacement \\0 is the whole match; $N also works" {
    try expectReplace("\\d+", "x42y", "[\\0]", "x[42]y");
    try expectReplace("(\\d)(\\d)", "ab12cd", "$2$1", "ab21cd");
}

test "regex: global replace + empty-match handling (no infinite loop)" {
    try expectReplace("o", "foo boo", "0", "f00 b00");
    // a* matches empty before 'b' and at end → leading/trailing inserts.
    try expectReplace("a*", "b", "X", "XbX");
    try expectReplace("x*", "héllo", "-", "-h-é-l-l-o-");
    try expectReplace("a*", "aaa", "X", "X");
    try expectReplace(".*", "abcd", "xx", "xx");
    try expectReplace("b*", "abc", "-", "-a-c-");
}

fn expectMatchWith(pattern: []const u8, options: Options, input: []const u8, should_match: bool) !void {
    var re = try Regex.compileWith(std.testing.allocator, pattern, options);
    defer re.deinit();
    var scratch = Scratch.init(std.testing.allocator);
    defer scratch.deinit();
    const slots = try std.testing.allocator.alloc(?usize, re.n_slots);
    defer std.testing.allocator.free(slots);
    try std.testing.expectEqual(should_match, try re.findWith(&scratch, input, 0, slots));
    try std.testing.expectEqual(should_match, try re.matchesWith(&scratch, input, 0));
}

test "regex: options fold ASCII case and let dot cross newlines" {
    const cases = .{
        .{ "b", Options{}, "ABC", false },
        .{ "b", Options{ .case_insensitive = true }, "ABC", true },
        .{ "[a-c]", Options{ .case_insensitive = true }, "B", true },
        .{ "[^b]", Options{ .case_insensitive = true }, "B", false },
        .{ "a.b", Options{}, "a\nb", false },
        .{ "a.b", Options{ .dot_all = true }, "a\nb", true },
    };
    inline for (cases) |c| try expectMatchWith(c[0], c[1], c[2], c[3]);
}

test "regex: ^ and $ hold at the text's ends, and at line ends when multiline" {
    const ml = Options{ .multiline = true };
    const cases = .{
        .{ "^b", Options{}, "a\nb", false },
        .{ "^b", ml, "a\nb", true },
        .{ "a$", Options{}, "a\nb", false },
        .{ "a$", ml, "a\nb", true },
        .{ "^a$", Options{}, "a\n", false },
        .{ "^a$", ml, "a\n", true },
        .{ "^a", Options{}, "a\nb", true },
        .{ "b$", Options{}, "a\nb", true },
        .{ "x|^b", Options{}, "a\nb", false },
        .{ "x|^b", ml, "a\nb", true },
        .{ "c$|x", Options{}, "c\nd", false },
        .{ "c$|x", ml, "c\nd", true },
        .{ "^[a-z]+$", Options{}, "ab\ncd", false },
        .{ "^[a-z]+$", ml, "ab\ncd", true },
    };
    inline for (cases) |c| try expectMatchWith(c[0], c[1], c[2], c[3]);
}

test "regex: findNth counts matches from a start byte" {
    var re = try Regex.compile(std.testing.allocator, "dog");
    defer re.deinit();
    var scratch = Scratch.init(std.testing.allocator);
    defer scratch.deinit();
    const slots = try std.testing.allocator.alloc(?usize, re.n_slots);
    defer std.testing.allocator.free(slots);
    const cases = .{
        .{ 0, 1, @as(?usize, 0) },
        .{ 0, 2, @as(?usize, 8) },
        .{ 1, 1, @as(?usize, 8) },
        .{ 0, 3, @as(?usize, null) },
    };
    inline for (cases) |c| {
        const found = try re.findNth(&scratch, "dog cat dog", c[0], c[1], slots);
        try std.testing.expectEqual(c[2] != null, found);
        if (found) try std.testing.expectEqual(c[2].?, slots[0].?);
    }
}

test "regex: replaceScratch keeps the prefix and replaces one occurrence" {
    var re = try Regex.compile(std.testing.allocator, "b");
    defer re.deinit();
    var scratch = Scratch.init(std.testing.allocator);
    defer scratch.deinit();
    const cases = .{
        .{ 0, 0, "aXc aXc aXc" },
        .{ 0, 2, "abc aXc abc" },
        .{ 2, 1, "abc aXc abc" },
        .{ 0, 5, "abc abc abc" },
    };
    inline for (cases) |c| {
        try std.testing.expectEqualStrings(c[2], try re.replaceScratch("abc abc abc", "X", &scratch, c[0], c[1]));
    }
}

test "regex: anchors bind to whole-string boundaries" {
    try expectMatch("^abc$", "abc", true);
    try expectMatch("^abc$", "xabc", false);
    try expectMatch("^abc$", "abcx", false);
    try expectMatch("bc$", "aabc", true);
}

// The stationary-run bulk-skip in `findWithImpl(true, ...)` must produce
// byte-for-byte the same match + capture slots as the reference matcher
// (`findWithImpl(false, ...)`) for every input and every start position.
// This differential test is the correctness contract for that fast path:
// it spans loops with one and multiple survivors, capturing vs
// non-capturing loops (where survivor capture pointers do/don't change),
// lazy quantifiers, `\b` (which disables the skip), embedded newlines
// (mid-string `^` seed points), and unanchored patterns.
test "regex: stationary bulk-skip equals reference matcher (fuzz)" {
    const patterns = [_][]const u8{
        "^https?://(?:www\\.)?([^/]+)/.*$",
        "^[^/]+/.*$",
        "^[^/]+$",
        "^.*$",
        "^.*?b$",
        "^a+b+$",
        "^[ab]+c$",
        "^[^c]+c$",
        "^[a-y]*z$",
        "^(a)+$",
        "^(?:ab)+$",
        "^x[^/]+y$",
        "^\\w+$",
        "^\\d+\\.\\d+$",
        "^a*a*b$",
        "^[^x]*x[^y]*y$",
        "\\bcat\\b",
        "^\\bword\\b$",
        "[^/]+",
        "abc",
        "^(cat|dog)+$",
        // Unanchored — exercise the first-byte prefilter.
        "google",
        "utm[a-z]*",
        "a*b",
        "(cat|dog)",
        "[0-9]+",
        "x[ab]+y",
        "ab+c",
        ".*",
        // Dense continue-sets — exercise the bulk-skip's scalar fallback.
        "^[a-z]+$",
        "^[a-y]+z$",
        "^[0-9]+x$",
        "^[ -~]+$",
    };
    const alphabet = "abc/xyz12. \nwd_goletum";
    var prng = std.Random.DefaultPrng.init(0xC0FFEE_1234);
    const rnd = prng.random();
    var buf: [40]u8 = undefined;

    for (patterns) |pat| {
        var re = try Regex.compile(std.testing.allocator, pat);
        defer re.deinit();
        var s_fast = Scratch.init(std.testing.allocator);
        defer s_fast.deinit();
        var s_slow = Scratch.init(std.testing.allocator);
        defer s_slow.deinit();
        const slots_fast = try std.testing.allocator.alloc(?usize, re.n_slots);
        defer std.testing.allocator.free(slots_fast);
        const slots_slow = try std.testing.allocator.alloc(?usize, re.n_slots);
        defer std.testing.allocator.free(slots_slow);

        var iter: usize = 0;
        while (iter < 600) : (iter += 1) {
            const len = rnd.intRangeAtMost(usize, 0, buf.len);
            for (buf[0..len]) |*b| b.* = alphabet[rnd.intRangeLessThan(usize, 0, alphabet.len)];
            const input = buf[0..len];

            var start: usize = 0;
            while (start <= len) : (start += 1) {
                @memset(slots_fast, null);
                @memset(slots_slow, null);
                const mf = try re.findWithImpl(true, &s_fast, input, start, slots_fast);
                const ms = try re.findWithImpl(false, &s_slow, input, start, slots_slow);
                try std.testing.expectEqual(ms, mf);
                if (ms) try std.testing.expectEqualSlices(?usize, slots_slow, slots_fast);
            }
        }
    }
}

test "regex: DFA matchesWith basics" {
    const cases = .{
        .{ .pat = "^https?://(?:www\\.)?([^/]+)/.*$", .yes = "https://www.example.com/path", .no = "ftp://x/" },
        .{ .pat = "[0-9]+", .yes = "abc42x", .no = "abcdef" },
        .{ .pat = "a*", .yes = "", .no = "" }, // matches empty anywhere — no counterexample exists
        .{ .pat = "^$", .yes = "", .no = "x" },
        .{ .pat = "(cat|dog)", .yes = "hotdog!", .no = "bird" },
        .{ .pat = "\\bcat\\b", .yes = "a cat.", .no = "scatter" }, // VM fallback (word boundary)
    };
    inline for (cases) |c| {
        var re = try Regex.compile(std.testing.allocator, c.pat);
        defer re.deinit();
        var scratch = Scratch.init(std.testing.allocator);
        defer scratch.deinit();
        try std.testing.expect(try re.matchesWith(&scratch, c.yes, 0));
        if (c.no.len > 0) try std.testing.expect(!try re.matchesWith(&scratch, c.no, 0));
    }
}

// The DFA's existence answer must equal the reference Pike VM's for every
// pattern (including word-boundary patterns, which exercise the VM
// fallback), every input, and every start position. Same corpus as the
// bulk-skip differential above — this is the DFA's correctness contract.
test "regex: DFA matchesWith equals reference matcher (fuzz)" {
    const patterns = [_][]const u8{
        "^https?://(?:www\\.)?([^/]+)/.*$",
        "^[^/]+/.*$",
        "^[^/]+$",
        "^.*$",
        "^.*?b$",
        "^a+b+$",
        "^[ab]+c$",
        "^[^c]+c$",
        "^[a-y]*z$",
        "^(a)+$",
        "^(?:ab)+$",
        "^x[^/]+y$",
        "^\\w+$",
        "^\\d+\\.\\d+$",
        "^a*a*b$",
        "^[^x]*x[^y]*y$",
        "\\bcat\\b",
        "^\\bword\\b$",
        "[^/]+",
        "abc",
        "^(cat|dog)+$",
        "google",
        "utm[a-z]*",
        "a*b",
        "(cat|dog)",
        "[0-9]+",
        "x[ab]+y",
        "ab+c",
        ".*",
        "^[a-z]+$",
        "^[a-y]+z$",
        "^[0-9]+x$",
        "^[ -~]+$",
        // Stress the seed-at-every-position semantics + eol mid-string.
        "[a-z]+[0-9]",
        "x$",
        "^x",
        "(a|ab)(c|bcd)",
    };
    const alphabet = "abc/xyz12. \nwd_goletum";
    var prng = std.Random.DefaultPrng.init(0xD0FA_2026);
    const rnd = prng.random();
    var buf: [40]u8 = undefined;

    for (patterns) |pat| {
        var re = try Regex.compile(std.testing.allocator, pat);
        defer re.deinit();
        var s_dfa = Scratch.init(std.testing.allocator);
        defer s_dfa.deinit();
        var s_ref = Scratch.init(std.testing.allocator);
        defer s_ref.deinit();
        const slots = try std.testing.allocator.alloc(?usize, re.n_slots);
        defer std.testing.allocator.free(slots);

        var iter: usize = 0;
        while (iter < 400) : (iter += 1) {
            const len = rnd.intRangeAtMost(usize, 0, buf.len);
            for (buf[0..len]) |*b| b.* = alphabet[rnd.intRangeLessThan(usize, 0, alphabet.len)];
            const input = buf[0..len];

            var start: usize = 0;
            while (start <= len) : (start += 1) {
                @memset(slots, null);
                const ref = try re.findWithImpl(false, &s_ref, input, start, slots);
                const dfa = try re.matchesWith(&s_dfa, input, start);
                try std.testing.expectEqual(ref, dfa);
            }
        }
    }
}

// The backtracker must report the reference Pike VM's exact match and
// captures for every pattern, option set, input, and start position. The
// corpus leans on what priority order decides: nested and empty-matching
// loops, lazy vs greedy, overlapping alternations, and star runs cut short
// by pairs an earlier start already explored.
test "regex: backtracker equals reference matcher (fuzz)" {
    const patterns = [_][]const u8{
        "^https?://(?:www\\.)?([^/]+)/.*$",
        "^([^/]+)/(.*)$",
        "(a*)*b",
        "(a|)*c",
        "(a*)+$",
        "^(a|ab)*c",
        "(a+?)(b*)",
        "a{2,4}",
        "(x|xy){1,3}z",
        "((a)|b)+",
        "a*?",
        "(?:a|b)*?c",
        ".*?x",
        "^(.*)/(.*)$",
        "([a-c]*)([a-c]*)",
        "\\b\\w+\\b",
        "(a|b|)+",
        "(|a)+",
        "x*",
        "^$",
        "$",
        "^",
        "[^/]+",
        "(cat|dog)",
        "a*b",
        "x[ab]+y",
        "(a|ab)(c|bcd)",
        "[a-z]+[0-9]",
        "^[^x]*x[^y]*y$",
        "(\\w+)\\.(\\w+)",
        "([^ ]*) ([^ ]*)",
        "(.*)(\\d+)",
        "\\B.\\B",
        "(a*)(a*)(a*)",
        "^(.*?)(.*)$",
        "^ab?c?(?:ww\\.)?([^/]+)/.*$",
        "^(?:wd)?([^/]*)(/.*)?$",
        "(?:x|xy)?z?([^ ]+) ",
        "a?(b?)c?\\.",
        "([^.]*)\\.([^.]*)$",
    };
    const option_sets = [_]Options{
        .{},
        .{ .multiline = true },
        .{ .dot_all = true, .case_insensitive = true },
    };
    const alphabet = "abc/xyz12. \nwd_AB";
    var prng = std.Random.DefaultPrng.init(0xBAC7_2026);
    const rnd = prng.random();
    var buf: [40]u8 = undefined;
    var searches: u64 = 0;
    var overruns: u64 = 0;

    for (option_sets) |options| for (patterns) |pat| {
        var re = try Regex.compileWith(std.testing.allocator, pat, options);
        defer re.deinit();
        var s_bt = Scratch.init(std.testing.allocator);
        defer s_bt.deinit();
        var s_memo = Scratch.init(std.testing.allocator);
        defer s_memo.deinit();
        var s_ref = Scratch.init(std.testing.allocator);
        defer s_ref.deinit();
        const slots_bt = try std.testing.allocator.alloc(?usize, re.n_slots);
        defer std.testing.allocator.free(slots_bt);
        const slots_ref = try std.testing.allocator.alloc(?usize, re.n_slots);
        defer std.testing.allocator.free(slots_ref);

        var iter: usize = 0;
        while (iter < 300) : (iter += 1) {
            const len = rnd.intRangeAtMost(usize, 0, buf.len);
            for (buf[0..len]) |*b| b.* = alphabet[rnd.intRangeLessThan(usize, 0, alphabet.len)];
            const input = buf[0..len];

            var start: usize = 0;
            while (start <= len) : (start += 1) {
                @memset(slots_ref, null);
                const ref = try re.findWithImpl(false, &s_ref, input, start, slots_ref);
                // `s_bt` always starts without the memo; `s_memo` is held
                // to the memoized search alone.
                s_bt.bt_tries = 0;
                s_bt.bt_overruns = 0;
                s_memo.bt_prog = @intFromPtr(re.prog.ptr);
                s_memo.bt_tries = 64;
                s_memo.bt_overruns = 64;
                for ([_]*Scratch{ &s_bt, &s_memo }) |s| {
                    @memset(slots_bt, null);
                    const bt = (try re.findBacktrack(s, input, start, slots_bt)).?;
                    try std.testing.expectEqual(ref, bt);
                    if (ref) try std.testing.expectEqualSlices(?usize, slots_ref, slots_bt);
                }
                searches += 1;
                overruns += s_bt.bt_overruns;
            }
        }
    };
    // Both ways the search without a memo ends were exercised.
    try std.testing.expect(overruns > 0 and overruns < searches);
}

test "regex: findAnyByte finds the first of several bytes at every offset" {
    var prng = std.Random.DefaultPrng.init(0xF1ED_2026);
    const rnd = prng.random();
    var buf: [70]u8 = undefined;
    const sets = [_][]const u8{ "/", "\n", "/?#", "abcd", "" };
    var iter: usize = 0;
    while (iter < 2000) : (iter += 1) {
        const len = rnd.intRangeAtMost(usize, 0, buf.len);
        for (buf[0..len]) |*b| b.* = "xyz/?#\nabcd"[rnd.intRangeLessThan(usize, 0, 11)];
        const input = buf[0..len];
        for (sets) |set| {
            var from: usize = 0;
            while (from <= len) : (from += 1) {
                const want = std.mem.indexOfAnyPos(u8, input, from, set) orelse len;
                try std.testing.expectEqual(want, findAnyByte(input, from, set));
            }
        }
    }
}

test "regex: backtracker defers oversized windows to the Pike VM" {
    var re = try Regex.compile(std.testing.allocator, "(a|b)*c");
    defer re.deinit();
    var scratch = Scratch.init(std.testing.allocator);
    defer scratch.deinit();
    scratch.bt_prog = @intFromPtr(re.prog.ptr);
    scratch.bt_tries = 64;
    scratch.bt_overruns = 64;
    const big = try std.testing.allocator.alloc(u8, BACKTRACK_MAX_BITS / re.bt.rows + 1);
    defer std.testing.allocator.free(big);
    @memset(big, 'a');
    big[big.len - 1] = 'c';
    var slots: [4]?usize = undefined;
    try std.testing.expectEqual(@as(?bool, null), try re.findBacktrack(&scratch, big, 0, &slots));
    try std.testing.expect(try re.findWith(&scratch, big, 0, &slots));
    try std.testing.expectEqual(@as(?usize, 0), slots[0]);
    try std.testing.expectEqual(@as(?usize, big.len), slots[1]);
}

test "regex: backtracker without a memo takes oversized windows it finishes in budget" {
    var re = try Regex.compile(std.testing.allocator, "^([^/]+)/(.*)$");
    defer re.deinit();
    var scratch = Scratch.init(std.testing.allocator);
    defer scratch.deinit();
    const big = try std.testing.allocator.alloc(u8, BACKTRACK_MAX_BITS / re.bt.rows + 1);
    defer std.testing.allocator.free(big);
    @memset(big, 'a');
    big[big.len / 2] = '/';
    var slots: [6]?usize = undefined;
    try std.testing.expectEqual(@as(?bool, true), try re.findBacktrack(&scratch, big, 0, &slots));
    try std.testing.expectEqual(@as(?usize, big.len / 2), slots[3]);
    try std.testing.expectEqual(@as(?usize, big.len), slots[5]);
    try std.testing.expectEqual(@as(u64, 0), scratch.bt_overruns);
}

// Diagnostic micro-benchmark: isolate Pike-VM regex throughput from the query
// engine. Builds a flat array of synthetic Referer-like URLs (contiguous,
// best-case locality) and runs the EXACT `replaceAllScratch` the query uses,
// 1/2/4/8 threads, each thread its own compiled Regex + Scratch (mirrors the
// per-worker Computes). Pattern uses [.] for the literal dot and a literal
// replacement (the match cost — the bottleneck — is identical to `\1`; only the
// trivial substitution differs). Gated behind THINDB_REGEX_BENCH so the normal
// suite skips it. Build ReleaseFast, then run the test exe with the env set.
test "regex throughput micro-bench" {
    // Diagnostic only — flip to true and build ReleaseFast to measure raw
    // Pike-VM throughput + thread scaling outside the query engine.
    const run_bench = false;
    if (!run_bench) return;
    const prof = @import("prof.zig");
    const A = std.heap.page_allocator;
    const N: usize = 3_000_000;

    var data: std.ArrayListUnmanaged(u8) = .empty;
    defer data.deinit(A);
    try data.ensureTotalCapacity(A, N * 80);
    const offsets = try A.alloc(usize, N + 1);
    defer A.free(offsets);

    var prng = std.Random.DefaultPrng.init(0xC0FFEE);
    const rnd = prng.random();
    const hosts = [_][]const u8{ "example.com", "news.site.org", "shop.bigstore.co.uk", "a.b", "verylongsubdomain.example.museum", "cdn.assets.net" };
    var k: usize = 0;
    while (k < N) : (k += 1) {
        offsets[k] = data.items.len;
        try data.appendSlice(A, if (rnd.boolean()) "https://" else "http://");
        if (rnd.boolean()) try data.appendSlice(A, "www.");
        try data.appendSlice(A, hosts[rnd.uintLessThan(usize, hosts.len)]);
        try data.append(A, '/');
        const path_len = rnd.uintLessThan(usize, 80);
        var p: usize = 0;
        while (p < path_len) : (p += 1) {
            try data.append(A, if (p % 8 == 7) '/' else @as(u8, 'a') + rnd.uintLessThan(u8, 26));
        }
    }
    offsets[N] = data.items.len;
    const total_bytes = data.items.len;
    std.debug.print("\n[regex-bench] {d} urls, {d} bytes (avg {d:.1} B/url)\n", .{ N, total_bytes, @as(f64, @floatFromInt(total_bytes)) / @as(f64, @floatFromInt(N)) });

    const PATTERN = "^https?://(?:www[.])?([^/]+)/.*$";
    const Job = struct { data: []const u8, offsets: []const usize, lo: usize, hi: usize, checksum: *usize };
    const worker = struct {
        fn run(job: Job) void {
            var re = Regex.compile(std.heap.page_allocator, PATTERN) catch return;
            defer re.deinit();
            var scratch = Scratch.init(std.heap.page_allocator);
            defer scratch.deinit();
            var cs: usize = 0;
            var i = job.lo;
            while (i < job.hi) : (i += 1) {
                const url = job.data[job.offsets[i]..job.offsets[i + 1]];
                const outp = re.replaceAllScratch(url, "x", &scratch) catch break;
                cs +%= outp.len;
            }
            job.checksum.* = cs;
        }
    }.run;

    inline for (.{ 1, 2, 4, 8 }) |dop| {
        var checksums: [dop]usize = [_]usize{0} ** dop;
        var threads: [dop]std.Thread = undefined;
        const t0 = prof.nowTicks();
        for (0..dop) |t| {
            const lo = t * N / dop;
            const hi = (t + 1) * N / dop;
            threads[t] = try std.Thread.spawn(.{}, worker, .{Job{ .data = data.items, .offsets = offsets, .lo = lo, .hi = hi, .checksum = &checksums[t] }});
        }
        for (0..dop) |t| threads[t].join();
        const ms = prof.ticksToMs(prof.nowTicks() - t0);
        var total_cs: usize = 0;
        for (checksums) |c| total_cs += c;
        std.mem.doNotOptimizeAway(total_cs);
        const secs = ms / 1000.0;
        const mbps = @as(f64, @floatFromInt(total_bytes)) / secs / 1e6;
        const us_per = ms * 1000.0 / @as(f64, @floatFromInt(N));
        const mrps = @as(f64, @floatFromInt(N)) / secs / 1e6;
        std.debug.print("[regex-bench] dop={d}: {d:.0}ms  {d:.0} MB/s  {d:.2} M urls/s  {d:.3} us/url\n", .{ dop, ms, mbps, mrps, us_per });
    }
}
