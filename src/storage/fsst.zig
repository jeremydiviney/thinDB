//! FSST — Fast Static Symbol Table string compression (Boncz, Neumann,
//! Mühleisen, VLDB 2020). Replaces frequent 1-8 byte substrings with 1-byte
//! codes from a table of up to 255 symbols; code 255 escapes one literal byte.
//!
//! Properties this codebase relies on:
//!   - Per-string random access: every string encodes/decodes independently —
//!     no shared stream state — so a block can decode one row without
//!     touching the rest.
//!   - Deterministic encode: greedy longest-match with a fixed tie-break,
//!     so equal plaintexts under the SAME table produce identical compressed
//!     bytes — compressed-domain equality (per segment) is a memcmp.
//!   - Decode is a table lookup + short copy per code: the hot direction.
//!
//! Encode speed is secondary (it runs at flush/compaction), decode speed and
//! ratio are primary. The table is built once per segment per column from a
//! sample and stored with the segment.

const std = @import("std");
const Allocator = std.mem.Allocator;

/// Code 255 marks an escape: the next input byte is a literal.
pub const escape_code: u8 = 255;
pub const max_symbols: usize = 255;
pub const max_symbol_len: usize = 8;

/// Serialized table size bound: u8 count + lens + symbol bytes.
pub const max_serialized_size: usize = 1 + max_symbols + max_symbols * max_symbol_len;

/// Worst-case encoded size for `n` input bytes (every byte escapes).
pub fn encodedSizeBound(n: usize) usize {
    return n * 2;
}

/// Worst-case decoded size for `n` compressed bytes (every code expands to a
/// full 8-byte symbol). Lets per-row consumers size a scratch in O(1) and
/// decode in one pass instead of walking the codes twice (`decodedLen` +
/// `decodeInto`).
pub fn decodedSizeBound(n: usize) usize {
    return n * max_symbol_len;
}

/// Decode acceleration entry: symbol bytes + advance length packed into one
/// 16-byte cell so the hot loop touches a single cache line per code (the
/// split lens/bytes arrays cost two loads). `len` is u64 purely for layout.
pub const DecEntry = extern struct {
    sym: [max_symbol_len]u8,
    len: u64,
};

pub const SymbolTable = struct {
    n_symbols: u16 = 0,
    lens: [max_symbols]u8 = @splat(0),
    bytes: [max_symbols][max_symbol_len]u8 = @splat(@splat(0)),

    /// Encoder index, built by `finalize`: for each first byte `b`, the codes
    /// of symbols starting with `b`, longest first (ties broken bytewise so
    /// encode is deterministic). `cand_start[b]..cand_start[b+1]` slices
    /// `cand_codes`.
    cand_start: [257]u16 = @splat(0),
    cand_codes: [max_symbols]u8 = @splat(0),

    /// Decode acceleration table (built by `finalize`).
    dec: [max_symbols]DecEntry = @splat(.{ .sym = @splat(0), .len = 0 }),

    pub fn symbol(self: *const SymbolTable, code: u8) []const u8 {
        return self.bytes[code][0..self.lens[code]];
    }

    /// Build the per-first-byte encoder index and the decode acceleration
    /// table. Must be called after the lens/bytes are populated (deserialize
    /// and buildTable both do).
    pub fn finalize(self: *SymbolTable) void {
        for (0..self.n_symbols) |c| {
            self.dec[c] = .{ .sym = self.bytes[c], .len = self.lens[c] };
        }
        var counts: [256]u16 = @splat(0);
        for (0..self.n_symbols) |c| counts[self.bytes[c][0]] += 1;
        var acc: u16 = 0;
        for (0..256) |b| {
            self.cand_start[b] = acc;
            acc += counts[b];
        }
        self.cand_start[256] = acc;
        var fill: [256]u16 = undefined;
        @memcpy(&fill, self.cand_start[0..256]);
        for (0..self.n_symbols) |c| {
            const b = self.bytes[c][0];
            self.cand_codes[fill[b]] = @intCast(c);
            fill[b] += 1;
        }
        // Longest-first within a first byte; ties bytewise, then by code —
        // a total, deterministic order.
        for (0..256) |b| {
            const lo = self.cand_start[b];
            const hi = self.cand_start[b + 1];
            std.sort.pdq(u8, self.cand_codes[lo..hi], self, candLess);
        }
    }

    fn candLess(self: *SymbolTable, a: u8, b: u8) bool {
        if (self.lens[a] != self.lens[b]) return self.lens[a] > self.lens[b];
        return switch (std.mem.order(u8, self.symbol(a), self.symbol(b))) {
            .lt => true,
            .gt => false,
            .eq => a < b,
        };
    }

    /// Longest symbol matching a prefix of `rest`, or null (escape the byte).
    pub fn match(self: *const SymbolTable, rest: []const u8) ?u8 {
        const lo = self.cand_start[rest[0]];
        const hi = self.cand_start[@as(usize, rest[0]) + 1];
        for (self.cand_codes[lo..hi]) |c| {
            const l = self.lens[c];
            if (l <= rest.len and std.mem.eql(u8, self.bytes[c][0..l], rest[0..l])) {
                return c;
            }
        }
        return null;
    }

    /// Append the encoding of `src` to `dst`. Caller must have reserved
    /// `encodedSizeBound(src.len)` of unused capacity.
    pub fn encodeAppend(self: *const SymbolTable, src: []const u8, dst: *std.ArrayListUnmanaged(u8)) void {
        var i: usize = 0;
        while (i < src.len) {
            if (self.match(src[i..])) |c| {
                dst.appendAssumeCapacity(c);
                i += self.lens[c];
            } else {
                dst.appendAssumeCapacity(escape_code);
                dst.appendAssumeCapacity(src[i]);
                i += 1;
            }
        }
    }

    /// Decoded byte length of `src` without materializing it.
    pub fn decodedLen(self: *const SymbolTable, src: []const u8) usize {
        var i: usize = 0;
        var n: usize = 0;
        while (i < src.len) {
            const c = src[i];
            if (c == escape_code) {
                n += 1;
                i += 2;
            } else {
                n += self.lens[c];
                i += 1;
            }
        }
        return n;
    }

    /// Decode `src` into `dst`, returning the decoded length. `dst` must hold
    /// exactly the decoded bytes (no slack needed): the hot path blindly
    /// copies 8 bytes per code while there's room and falls back to an
    /// exact-length copy near the buffer end.
    pub fn decodeInto(self: *const SymbolTable, src: []const u8, dst: []u8) usize {
        var i: usize = 0;
        var o: usize = 0;
        while (i < src.len) {
            const c = src[i];
            if (c == escape_code) {
                dst[o] = src[i + 1];
                o += 1;
                i += 2;
            } else {
                const e = &self.dec[c];
                if (o + max_symbol_len <= dst.len) {
                    dst[o..][0..max_symbol_len].* = e.sym;
                } else {
                    const l: usize = @intCast(e.len);
                    @memcpy(dst[o..][0..l], e.sym[0..l]);
                }
                o += @intCast(e.len);
                i += 1;
            }
        }
        return o;
    }

    /// Bounds-unchecked sibling of `decodeInto` for the per-row hot paths:
    /// `dst.len` MUST be at least `decodedSizeBound(src.len)`, which makes the
    /// blind 8-byte copy provably in-bounds for every code (each consumed
    /// compressed byte expands to at most 8 output bytes), so the loop carries
    /// no per-code capacity branch.
    pub fn decodeIntoUnchecked(self: *const SymbolTable, src: []const u8, dst: []u8) usize {
        std.debug.assert(dst.len >= decodedSizeBound(src.len));
        var i: usize = 0;
        var o: usize = 0;
        while (i < src.len) {
            const c = src[i];
            if (c == escape_code) {
                dst[o] = src[i + 1];
                o += 1;
                i += 2;
            } else {
                const e = &self.dec[c];
                dst[o..][0..max_symbol_len].* = e.sym;
                o += @intCast(e.len);
                i += 1;
            }
        }
        return o;
    }

    /// Stream-decode `src`, feeding decoded bytes to `sink.update(bytes)`
    /// without materializing the string. For digest-while-decode consumers.
    pub fn decodeStream(self: *const SymbolTable, src: []const u8, sink: anytype) void {
        var i: usize = 0;
        while (i < src.len) {
            const c = src[i];
            if (c == escape_code) {
                sink.update(src[i + 1 ..][0..1]);
                i += 2;
            } else {
                sink.update(self.bytes[c][0..self.lens[c]]);
                i += 1;
            }
        }
    }

    pub fn serializedSize(self: *const SymbolTable) usize {
        var n: usize = 1 + self.n_symbols;
        for (0..self.n_symbols) |c| n += self.lens[c];
        return n;
    }

    /// Layout: [n_symbols u8][lens × n][symbol bytes, concatenated].
    pub fn serialize(self: *const SymbolTable, out: []u8) usize {
        out[0] = @intCast(self.n_symbols);
        var o: usize = 1;
        for (0..self.n_symbols) |c| {
            out[o] = self.lens[c];
            o += 1;
        }
        for (0..self.n_symbols) |c| {
            @memcpy(out[o..][0..self.lens[c]], self.bytes[c][0..self.lens[c]]);
            o += self.lens[c];
        }
        return o;
    }

    pub fn deserialize(data: []const u8) error{CorruptSymbolTable}!SymbolTable {
        if (data.len < 1) return error.CorruptSymbolTable;
        const n: usize = data[0];
        if (n > max_symbols or data.len < 1 + n) return error.CorruptSymbolTable;
        var st = SymbolTable{ .n_symbols = @intCast(n) };
        var o: usize = 1 + n;
        for (0..n) |c| {
            const l = data[1 + c];
            if (l == 0 or l > max_symbol_len or o + l > data.len) return error.CorruptSymbolTable;
            st.lens[c] = l;
            @memcpy(st.bytes[c][0..l], data[o..][0..l]);
            o += l;
        }
        st.finalize();
        return st;
    }
};

// ---------------------------------------------------------------------------
// Table construction — iterative greedy over a sample (paper §4.4)
// ---------------------------------------------------------------------------

/// A counting "unit" is what one greedy step consumes: an existing symbol
/// (unit = 256 + code) or an escaped literal byte (unit = byte value).
const n_units: usize = 256 + max_symbols;

const Candidate = struct {
    len: u8,
    bytes: [max_symbol_len]u8,
    gain: u64,
};

const build_iterations = 5;

/// Build a symbol table from sample strings. The sample should be a few tens
/// of KB drawn across the data (caller's policy); build cost is
/// `build_iterations` greedy passes over it plus a pair-table sweep.
pub fn buildTable(allocator: Allocator, sample: []const []const u8) !SymbolTable {
    var st = SymbolTable{};
    st.finalize(); // empty table: everything escapes on the first pass

    const count1 = try allocator.alloc(u32, n_units);
    defer allocator.free(count1);
    const count2 = try allocator.alloc(u32, n_units * n_units);
    defer allocator.free(count2);

    var cands: std.AutoHashMapUnmanaged(u128, u64) = .empty;
    defer cands.deinit(allocator);
    var selected: std.ArrayListUnmanaged(Candidate) = .empty;
    defer selected.deinit(allocator);

    for (0..build_iterations) |_| {
        @memset(count1, 0);
        @memset(count2, 0);

        // Greedy-parse the sample with the current table, counting consumed
        // units and adjacent unit pairs. Pairs never cross string boundaries
        // (symbols must not either — strings decode independently).
        for (sample) |s| {
            var prev: ?usize = null;
            var i: usize = 0;
            while (i < s.len) {
                var unit: usize = undefined;
                if (st.match(s[i..])) |c| {
                    unit = 256 + @as(usize, c);
                    i += st.lens[c];
                } else {
                    unit = s[i];
                    i += 1;
                }
                count1[unit] += 1;
                if (prev) |p| count2[p * n_units + unit] += 1;
                prev = unit;
            }
        }

        // Candidates: every consumed unit, plus every adjacent pair
        // concatenated (truncated to 8 bytes). Gain = occurrences × covered
        // length — the paper's heuristic.
        cands.clearRetainingCapacity();
        for (0..n_units) |u| {
            if (count1[u] == 0) continue;
            var buf: [max_symbol_len]u8 = @splat(0);
            const l = unitBytes(&st, u, &buf);
            try addGain(allocator, &cands, buf, l, @as(u64, count1[u]) * l);
        }
        for (0..n_units) |a| {
            if (count1[a] == 0) continue;
            var abuf: [max_symbol_len]u8 = @splat(0);
            const alen = unitBytes(&st, a, &abuf);
            for (0..n_units) |b| {
                const cnt = count2[a * n_units + b];
                if (cnt == 0) continue;
                var buf: [max_symbol_len]u8 = @splat(0);
                @memcpy(buf[0..alen], abuf[0..alen]);
                var bbuf: [max_symbol_len]u8 = @splat(0);
                const blen = unitBytes(&st, b, &bbuf);
                const take = @min(blen, max_symbol_len - alen);
                if (take == 0) continue;
                @memcpy(buf[alen..][0..take], bbuf[0..take]);
                const l: u8 = @intCast(alen + take);
                try addGain(allocator, &cands, buf, l, @as(u64, cnt) * l);
            }
        }

        // Keep the top `max_symbols` by gain (deterministic tie-break on the
        // packed key so identical samples build identical tables).
        selected.clearRetainingCapacity();
        var it = cands.iterator();
        while (it.next()) |e| {
            const key = e.key_ptr.*;
            const l: u8 = @intCast(key >> 64);
            var buf: [max_symbol_len]u8 = @splat(0);
            std.mem.writeInt(u64, &buf, @truncate(key), .big);
            try selected.append(allocator, .{ .len = l, .bytes = buf, .gain = e.value_ptr.* });
        }
        std.sort.pdq(Candidate, selected.items, {}, candGainDesc);

        st = SymbolTable{};
        const keep = @min(selected.items.len, max_symbols);
        for (selected.items[0..keep], 0..) |c, code| {
            st.lens[code] = c.len;
            st.bytes[code] = c.bytes;
        }
        st.n_symbols = @intCast(keep);
        st.finalize();
    }
    return st;
}

fn unitBytes(st: *const SymbolTable, unit: usize, out: *[max_symbol_len]u8) u8 {
    if (unit < 256) {
        out[0] = @intCast(unit);
        return 1;
    }
    const c: u8 = @intCast(unit - 256);
    const l = st.lens[c];
    @memcpy(out[0..l], st.bytes[c][0..l]);
    return l;
}

/// Key packs (len, first-8-bytes big-endian) so symbols dedupe exactly and
/// the sort tie-break is stable.
fn addGain(allocator: Allocator, cands: *std.AutoHashMapUnmanaged(u128, u64), buf: [max_symbol_len]u8, len: u8, gain: u64) !void {
    const key = (@as(u128, len) << 64) | std.mem.readInt(u64, &buf, .big);
    const g = try cands.getOrPut(allocator, key);
    if (!g.found_existing) g.value_ptr.* = 0;
    g.value_ptr.* += gain;
}

fn candGainDesc(_: void, a: Candidate, b: Candidate) bool {
    if (a.gain != b.gain) return a.gain > b.gain;
    if (a.len != b.len) return a.len > b.len;
    return std.mem.order(u8, &a.bytes, &b.bytes) == .lt;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const test_urls = [_][]const u8{
    "http://example.com/index.html",
    "http://example.com/products/widgets?id=123",
    "http://example.com/products/gadgets?id=456",
    "https://search.engine.example/query?q=hello+world",
    "https://search.engine.example/query?q=fsst+compression",
    "http://example.com/",
    "",
    "http://another.example.org/path/to/деталь", // non-ASCII bytes
    "http://example.com/products/widgets?id=124&ref=mail",
};

test "fsst round-trips a URL corpus through build/encode/decode" {
    const allocator = std.testing.allocator;
    const st = try buildTable(allocator, &test_urls);

    var enc: std.ArrayListUnmanaged(u8) = .empty;
    defer enc.deinit(allocator);
    var dec: std.ArrayListUnmanaged(u8) = .empty;
    defer dec.deinit(allocator);

    var total_raw: usize = 0;
    var total_enc: usize = 0;
    for (test_urls) |s| {
        enc.clearRetainingCapacity();
        try enc.ensureTotalCapacity(allocator, encodedSizeBound(s.len));
        st.encodeAppend(s, &enc);

        try std.testing.expectEqual(s.len, st.decodedLen(enc.items));
        dec.clearRetainingCapacity();
        try dec.resize(allocator, s.len + max_symbol_len);
        const n = st.decodeInto(enc.items, dec.items);
        try std.testing.expectEqualStrings(s, dec.items[0..n]);

        total_raw += s.len;
        total_enc += enc.items.len;
    }
    // The corpus shares long substrings; anything short of 30% savings means
    // the table build is broken.
    try std.testing.expect(total_enc * 10 < total_raw * 7);
}

test "fsst encode is deterministic and equality-preserving under one table" {
    const allocator = std.testing.allocator;
    const st = try buildTable(allocator, &test_urls);

    var a: std.ArrayListUnmanaged(u8) = .empty;
    defer a.deinit(allocator);
    var b: std.ArrayListUnmanaged(u8) = .empty;
    defer b.deinit(allocator);

    for (test_urls) |s| {
        a.clearRetainingCapacity();
        b.clearRetainingCapacity();
        try a.ensureTotalCapacity(allocator, encodedSizeBound(s.len));
        try b.ensureTotalCapacity(allocator, encodedSizeBound(s.len));
        st.encodeAppend(s, &a);
        st.encodeAppend(s, &b);
        try std.testing.expectEqualSlices(u8, a.items, b.items);
    }
    // Distinct plaintexts cannot collide (decode is a function of the
    // compressed bytes alone).
    for (test_urls, 0..) |s1, i| {
        for (test_urls[i + 1 ..]) |s2| {
            a.clearRetainingCapacity();
            b.clearRetainingCapacity();
            try a.ensureTotalCapacity(allocator, encodedSizeBound(s1.len));
            try b.ensureTotalCapacity(allocator, encodedSizeBound(s2.len));
            st.encodeAppend(s1, &a);
            st.encodeAppend(s2, &b);
            try std.testing.expect(!std.mem.eql(u8, a.items, b.items));
        }
    }
}

test "fsst symbol table serialization round-trips" {
    const allocator = std.testing.allocator;
    const st = try buildTable(allocator, &test_urls);

    var buf: [max_serialized_size]u8 = undefined;
    const n = st.serialize(&buf);
    try std.testing.expectEqual(st.serializedSize(), n);
    const back = try SymbolTable.deserialize(buf[0..n]);

    try std.testing.expectEqual(st.n_symbols, back.n_symbols);
    for (0..st.n_symbols) |c| {
        try std.testing.expectEqualSlices(u8, st.symbol(@intCast(c)), back.symbol(@intCast(c)));
    }
    // Same table ⇒ identical encodings.
    var a: std.ArrayListUnmanaged(u8) = .empty;
    defer a.deinit(allocator);
    var b: std.ArrayListUnmanaged(u8) = .empty;
    defer b.deinit(allocator);
    for (test_urls) |s| {
        a.clearRetainingCapacity();
        b.clearRetainingCapacity();
        try a.ensureTotalCapacity(allocator, encodedSizeBound(s.len));
        try b.ensureTotalCapacity(allocator, encodedSizeBound(s.len));
        st.encodeAppend(s, &a);
        back.encodeAppend(s, &b);
        try std.testing.expectEqualSlices(u8, a.items, b.items);
    }
}

test "fsst handles adversarial inputs: empty table, all-escape bytes, long runs" {
    const allocator = std.testing.allocator;

    // Empty sample → empty table → everything escapes, still round-trips.
    var st = try buildTable(allocator, &.{});
    try std.testing.expectEqual(@as(u16, 0), st.n_symbols);
    const s = "no symbols at all \xff\xfe\x00";
    var enc: std.ArrayListUnmanaged(u8) = .empty;
    defer enc.deinit(allocator);
    try enc.ensureTotalCapacity(allocator, encodedSizeBound(s.len));
    st.encodeAppend(s, &enc);
    try std.testing.expectEqual(s.len * 2, enc.items.len);
    var dec: [64]u8 = undefined;
    const n = st.decodeInto(enc.items, &dec);
    try std.testing.expectEqualStrings(s, dec[0..n]);

    // A run-heavy sample (worst case for pair growth) still round-trips and
    // compresses massively.
    const runs = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    st = try buildTable(allocator, &.{ runs, runs });
    enc.clearRetainingCapacity();
    try enc.ensureTotalCapacity(allocator, encodedSizeBound(runs.len));
    st.encodeAppend(runs, &enc);
    try std.testing.expect(enc.items.len <= runs.len / 4);
    var dec2: [128]u8 = undefined;
    const n2 = st.decodeInto(enc.items, &dec2);
    try std.testing.expectEqualStrings(runs, dec2[0..n2]);
}

test "fsst escape byte 0xff in input round-trips" {
    const allocator = std.testing.allocator;
    const tricky = "\xff\xff\xffabc\xff";
    const st = try buildTable(allocator, &.{ tricky, "abcabcabc" });
    var enc: std.ArrayListUnmanaged(u8) = .empty;
    defer enc.deinit(allocator);
    try enc.ensureTotalCapacity(allocator, encodedSizeBound(tricky.len));
    st.encodeAppend(tricky, &enc);
    var dec: [32]u8 = undefined;
    const n = st.decodeInto(enc.items, &dec);
    try std.testing.expectEqualStrings(tricky, dec[0..n]);
}

test "fsst decodeStream feeds the same bytes a materializing decode would" {
    const allocator = std.testing.allocator;
    const st = try buildTable(allocator, &test_urls);

    const Sink = struct {
        buf: std.ArrayListUnmanaged(u8) = .empty,
        a: Allocator,
        fn update(self: *@This(), bytes: []const u8) void {
            self.buf.appendSlice(self.a, bytes) catch unreachable;
        }
    };

    var enc: std.ArrayListUnmanaged(u8) = .empty;
    defer enc.deinit(allocator);
    for (test_urls) |s| {
        enc.clearRetainingCapacity();
        try enc.ensureTotalCapacity(allocator, encodedSizeBound(s.len));
        st.encodeAppend(s, &enc);
        var sink = Sink{ .a = allocator };
        defer sink.buf.deinit(allocator);
        st.decodeStream(enc.items, &sink);
        try std.testing.expectEqualStrings(s, sink.buf.items);
    }
}
