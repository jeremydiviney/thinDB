//! Minimal linear-time regular expression engine (Thompson NFA + Pike
//! VM with submatch tracking), in the RE2 family. No backtracking, so
//! matching is O(pattern × input) with no catastrophic blow-up (ReDoS).
//!
//! Supported syntax (the common POSIX/RE2 subset):
//!   literals, `.` (any byte except '\n'), `[...]` / `[^...]` classes
//!   with ranges, shorthands `\d \w \s \D \W \S`, escapes (`\.` `\n`
//!   `\t` `\\` etc), quantifiers `* + ? {n} {n,} {n,m}` (greedy and lazy
//!   `*? +? ??`), alternation `|`, capturing `( )` and non-capturing
//!   `(?: )` groups, anchors `^ $`, word boundaries `\b \B`.
//!
//! NOT supported (require backtracking — same omissions as RE2):
//!   in-pattern backreferences, lookahead/lookbehind. Backreferences in
//!   a *replacement* template (`\1`) are supported by `replaceAll` since
//!   that's plain substitution, not a pattern feature.
//!
//! Matching is byte-oriented (ASCII-aware). UTF-8 bytes flow through
//! `.` and negated classes fine; Unicode property classes (`\p{...}`)
//! and non-ASCII case folding are out of scope.

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const Error = error{RegexInvalidPattern} || Allocator.Error;

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
    fn contains(self: CharClass, b: u8) bool {
        return (self.bits[b >> 3] & (@as(u8, 1) << @intCast(b & 7))) != 0;
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
                    const parsed = try self.parseBraces();
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

    fn parseBraces(self: *Parser) Error!struct { min: u32, max: ?u32 } {
        _ = self.advance(); // '{'
        const min = try self.parseInt();
        var max: ?u32 = min;
        if (self.peek() == ',') {
            _ = self.advance();
            if (self.peek() == '}') {
                max = null; // {n,}
            } else {
                max = try self.parseInt();
            }
        }
        if (self.peek() != '}') return Error.RegexInvalidPattern;
        _ = self.advance();
        return .{ .min = min, .max = max };
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
                cc.add('\n');
                cc.negate(); // any byte except newline
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
            ')', '*', '+', '?', '{' => return Error.RegexInvalidPattern,
            else => {
                _ = self.advance();
                var cc: CharClass = .{};
                cc.add(c);
                return self.node(.{ .class = cc });
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
            else => {
                var cc: CharClass = .{};
                cc.add(unescapeByte(c));
                return self.node(.{ .class = cc });
            },
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
                if (negated) cc.negate();
                return self.node(.{ .class = cc });
            }
            first = false;
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
                    else => lo = unescapeByte(e),
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
                    hi = unescapeByte(e);
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
};

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

/// Reusable matcher scratch. Construct one per batch and pass it to every
/// `findWith` / `replaceAllScratch` call: the visited-stamp array, the two
/// thread lists, the seed buffer, and the per-match capture arena are all
/// retained across calls, so applying one compiled pattern to millions of
/// rows allocates essentially nothing in steady state.
pub const Scratch = struct {
    backing: Allocator,
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

    pub fn init(backing: Allocator) Scratch {
        return .{ .backing = backing, .arena = std.heap.ArenaAllocator.init(backing) };
    }

    pub fn deinit(self: *Scratch) void {
        self.arena.deinit();
        if (self.visited.len > 0) self.backing.free(self.visited);
        if (self.seed.len > 0) self.backing.free(self.seed);
        if (self.slots.len > 0) self.backing.free(self.slots);
        self.clist.deinit(self.backing);
        self.carried.deinit(self.backing);
        self.probe.deinit(self.backing);
        self.out_buf.deinit(self.backing);
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
    /// True when the program is `^`-anchored at the start (prog[1] is a
    /// `.bol` — no top-level alternation). Lets `find` seed the start
    /// thread only at line boundaries instead of at every position, which
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

    pub fn compile(allocator: Allocator, pattern: []const u8) Error!Regex {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();

        var parser = Parser{ .pat = pattern, .arena = arena.allocator() };
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

        return .{
            .prog = prog,
            .n_slots = 2 * parser.next_group,
            .allocator = allocator,
            .anchored_start = prog.len > 1 and prog[1] == .bol,
            .has_wordbound = has_wordbound,
            .loopy = loopy,
        };
    }

    pub fn deinit(self: *Regex) void {
        self.allocator.free(self.prog);
        self.allocator.free(self.loopy);
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
        return self.findWithImpl(true, scratch, input, start, out_slots);
    }

    /// `enable_skip` toggles the stationary-run bulk-skip fast path. The
    /// `false` instantiation is the reference matcher used by the
    /// differential property test to validate the `true` one; it is dead
    /// code in any non-test build.
    fn findWithImpl(self: *const Regex, comptime enable_skip: bool, scratch: *Scratch, input: []const u8, start: usize, out_slots: []?usize) Error!bool {
        try scratch.prepare(self.prog.len, self.n_slots, input.len);
        _ = scratch.arena.reset(.retain_capacity);

        const VM = struct {
            prog: []const Inst,
            input: []const u8,
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
                    .bol => if (pos == 0 or vm.input[pos - 1] == '\n') try vm.addThread(list, pc + 1, pos, caps),
                    .eol => if (pos == vm.input.len or vm.input[pos] == '\n') try vm.addThread(list, pc + 1, pos, caps),
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
            vm.gen += 1;
            clist.clearRetainingCapacity();
            // Carried threads (earlier-starting) get higher priority.
            for (carried.items) |t| try vm.addThread(clist, t.pc, sp, t.caps);
            // Seed an unanchored start thread at the lowest priority until a
            // match begins, so an earlier-starting match wins (leftmost). For
            // a `^`-anchored program the seed can only survive at a line
            // boundary, so skip it everywhere else — this removes the
            // per-position seed work that otherwise dominates anchored scans.
            if (matched == null and (!self.anchored_start or sp == 0 or input[sp - 1] == '\n')) {
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
        const allocator = scratch.backing;
        if (scratch.slots.len < self.n_slots) {
            scratch.slots = try scratch.backing.realloc(scratch.slots, self.n_slots);
        }
        const slots = scratch.slots[0..self.n_slots];

        const out = &scratch.out_buf;
        out.clearRetainingCapacity();

        var pos: usize = 0;
        while (pos <= input.len) {
            const found = try self.findWith(scratch, input, pos, slots);
            if (!found) break;
            const m_start = slots[0].?;
            const m_end = slots[1].?;
            // Copy the text before the match.
            try out.appendSlice(allocator, input[pos..m_start]);
            // Expand the template.
            try expandTemplate(allocator, out, template, input, slots);
            if (m_end > pos) {
                pos = m_end;
            } else {
                // Empty match: emit one byte and advance to avoid looping.
                if (m_end < input.len) try out.append(allocator, input[m_end]);
                pos = m_end + 1;
            }
        }
        if (pos < input.len) try out.appendSlice(allocator, input[pos..]);
        return out.items;
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
    };
    const alphabet = "abc/xyz12. \nwd_";
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
