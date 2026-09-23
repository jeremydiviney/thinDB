//! Write-ahead log. Every insert and delete is appended to a single
//! append-only file BEFORE the operation is applied to the memtable.
//! The fsync after that append is the durability point: once `insert`
//! returns success, the data survives a process / power crash.
//!
//! On reopen, the WAL is replayed to reconstruct the in-memory state
//! that was lost when the previous process exited (with whatever was
//! still in the memtable).
//!
//! File layout (v2, binary, little-endian; v1 remains readable):
//!
//!   Header (32 bytes):
//!     magic "tDBW"        4
//!     version u16         2
//!     flags u16           2  (reserved, 0)
//!     schema_fingerprint  8  (must match table's schema fingerprint on open)
//!     generation         16  (new on each replacement; absent in v1)
//!
//!   Sequence of records, each:
//!     type u8             1   (1=insert, 2=delete, 3=flush_marker,
//!                                  4=delete_expr, 5=replace)
//!     payload_len u32     4
//!     payload bytes       N
//!     checksum u64        8   (XxHash64 of [type ++ payload_len ++ payload])
//!
//! Insert payload:
//!     row_count u32
//!     per schema-column-order:
//!       optional null-bitmap bytes (only when nullable)
//!       value bytes (fixed-width packed, or string offset table + bytes)
//!
//! Delete payload:
//!     col_name_len u32 + col_name bytes
//!     op u8                (one of PredicateOp values)
//!     value_type u8        (one of ValueTag values)
//!     value bytes          (typed by value_type)
//!
//! Flush-marker payload:
//!     max_segment_id u64   (records BEFORE this marker are redundant)
//!
//! Replace payload (one UPDATE batch):
//!     retracted rows       (insert-payload layout)
//!     segment_id u64
//!     offset_count u32 + offset u32 each   (tombstoned in that segment)
//!     replacement rows     (insert-payload layout)

const std = @import("std");
const builtin = @import("builtin");
const Io = std.Io;
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const ValueTag = types.ValueTag;

const storage = @import("../storage/storage.zig");
const format = storage.format;

const memtable_mod = @import("memtable.zig");
const Memtable = memtable_mod.Memtable;
const ColumnStore = @import("store.zig").ColumnStore;

const codec = @import("wal_codec.zig");

pub const wal_magic: [4]u8 = .{ 't', 'D', 'B', 'W' };
pub const wal_version: u16 = 2;
pub const wal_filename = "wal";
pub const header_size: usize = 32;
const legacy_header_size: usize = 16;
pub const Checkpoint = storage.manifest.WalCheckpoint;
pub const record_header_size: usize = 1 + 4; // type + payload_len
pub const record_trailer_size: usize = 8; // xxhash64

/// Group-commit timing. The leader always spins briefly before fsync
/// (`coalesce_probe_ns`); if contention shows up during that probe (more
/// writers arrived at `awaitDurable`), it keeps spinning up to
/// `coalesce_max_ns` total, restarting the dwell clock each time a new
/// writer registers. This lets a single fsync cover an arbitrary burst
/// while bounding worst-case latency.
///
/// We can't use `Io.sleep` for these short waits — Windows's default
/// scheduler timer rounds sub-millisecond sleeps up to ~15.6 ms. So we
/// busy-wait via `Io.Clock.awake.now`. CPU would otherwise be idle during
/// the fsync syscall anyway.
///
/// Probe (20µs): small enough that the single-writer case pays negligible
/// extra latency on top of fsync (~250µs). Long enough that a writer
/// mid-append at T1 release has time to reach the coordinator.
pub const coalesce_probe_ns: u64 = 20_000;
/// Cap (200µs): hard upper bound on group-commit wait time. Bounds tail
/// latency even under continuous writer arrival.
pub const coalesce_max_ns: u64 = 200_000;

pub const RecordType = enum(u8) {
    insert = 1,
    delete = 2,
    flush_marker = 3,
    /// Rich-predicate delete — `DELETE FROM t WHERE <bool_expr>`. The
    /// payload encodes a `PredicateExpr` tree (AND/OR/NOT/leaf/etc).
    /// On replay, the memtable is filtered by evaluating the predicate
    /// over its rows. Segment-side tombstones are durable independently
    /// (per-segment atomic tmp+rename writes) — replay only fixes up
    /// the memtable.
    delete_expr = 4,
    /// One UPDATE batch: the memtable rows it retracts, the offsets it
    /// tombstones in one segment, and the rows that replace them. One record,
    /// so replay applies an UPDATE's deletes and their replacements together
    /// or not at all. Replay hands the offsets back to the table, which
    /// merges them into the segment's tombstone file.
    replace = 5,
};

/// Segment offsets that `replace` records tombstone, per segment id.
pub const SegmentTombstones = std.AutoArrayHashMapUnmanaged(u64, std.ArrayList(u32));

pub fn addSegmentTombstones(allocator: Allocator, map: *SegmentTombstones, segment_id: u64, offsets: []const u32) !void {
    if (offsets.len == 0) return;
    const entry = try map.getOrPut(allocator, segment_id);
    if (!entry.found_existing) entry.value_ptr.* = .empty;
    try entry.value_ptr.appendSlice(allocator, offsets);
}

pub fn deinitSegmentTombstones(allocator: Allocator, map: *SegmentTombstones) void {
    for (map.values()) |*offsets| offsets.deinit(allocator);
    map.deinit(allocator);
    map.* = .empty;
}

pub const Error = error{
    WalBadMagic,
    WalUnsupportedVersion,
    WalSchemaFingerprintMismatch,
    WalCorrupt,
    WalUnknownRecord,
    WalTooSmall,
    /// PredicateExpr contained a variant the WAL codec doesn't
    /// support (subqueries, var_refs, correlated forms). Caller can
    /// choose to skip WAL logging and proceed with the delete.
    WalPredicateUnsupported,
};

/// Owns the open file handle for the current WAL and accumulates writes.
/// One per Table. The `appendX` methods MUST be called serialized (i.e. under
/// the Table mutex) so file writes don't interleave; `awaitDurable` is
/// called WITHOUT the Table mutex so concurrent writers can amortize a single
/// fsync syscall (leader-follower group commit).
pub const WalWriter = struct {
    allocator: Allocator,
    io: Io,
    dir: Io.Dir,
    file: Io.File,

    /// Cumulative byte counter — monotonic across truncates. Used purely as
    /// a logical clock so `awaitDurable` knows whether a target write has
    /// been covered by some fsync. After a truncate, the physical file is
    /// small again but `write_offset` keeps advancing.
    write_offset: u64,
    physical_offset: u64 = header_size,
    generation: [16]u8 = @splat(0),
    /// Highest `write_offset` that has been durably fsynced. After truncate,
    /// this is bumped to `write_offset` (data before truncate is implicitly
    /// durable — either in a segment or no longer needed).
    synced_offset: u64,

    /// Coordinator state for leader-follower group commit. Held briefly
    /// during `writeRecord` to advance `write_offset`, and across the
    /// `awaitDurable` wait/signal protocol.
    coord_mu: Io.Mutex = .init,
    coord_cv: Io.Condition = .init,
    in_progress: bool = false,
    /// Number of followers currently parked in `coord_cv.wait`. The leader
    /// reads this when it claims the leader role; a non-zero value means
    /// there are pending writers whose bytes are already in the file, so
    /// the leader pauses briefly before snapshotting. That pause also gives
    /// any writer mid-append (just released `table.mutex`, racing toward
    /// `coord.mu`) time to bump `write_offset` so the leader's fsync covers
    /// them too. When `waiters == 0` the leader fsyncs immediately — no
    /// added latency for the single-writer case.
    waiters: u32 = 0,

    /// Diagnostic: number of times `awaitDurable` actually called
    /// `file.sync()` (i.e., this thread became the group-commit leader).
    /// Followers do not increment. Compare against the total `awaitDurable`
    /// call count to see the group-commit amortization ratio.
    fsync_count: usize = 0,
    /// Diagnostic: number of times the adaptive coalescing pause extended
    /// past the initial probe because other writers arrived. Compare with
    /// `fsync_count`: a high ratio means group commit is amortizing well.
    coalesce_count: usize = 0,

    pub fn create(
        allocator: Allocator,
        io: Io,
        dir: Io.Dir,
        schema_fingerprint: u64,
    ) !WalWriter {
        // Create-or-truncate. (Replay happens BEFORE create; the caller
        // either replayed and then truncated, or there was nothing to replay.)
        var file = try dir.createFile(io, wal_filename, .{});
        errdefer file.close(io);

        var generation: [16]u8 = undefined;
        io.random(&generation);
        const hdr = makeHeader(schema_fingerprint, generation);
        try file.writeStreamingAll(io, &hdr);
        try file.sync(io);
        try storage.syncDirectory(io, dir);

        return .{
            .allocator = allocator,
            .io = io,
            .dir = dir,
            .file = file,
            .write_offset = header_size,
            .synced_offset = header_size,
            .generation = generation,
        };
    }

    pub fn deinit(self: *WalWriter) void {
        self.file.close(self.io);
        self.* = undefined;
    }

    pub fn checkpoint(self: *const WalWriter) Checkpoint {
        return .{ .generation = self.generation, .offset = self.physical_offset };
    }

    fn makeHeader(schema_fingerprint: u64, generation: [16]u8) [header_size]u8 {
        var hdr: [header_size]u8 = undefined;
        @memcpy(hdr[0..4], &wal_magic);
        format.writeU16(hdr[4..6], wal_version);
        format.writeU16(hdr[6..8], 0);
        format.writeU64(hdr[8..16], schema_fingerprint);
        @memcpy(hdr[16..32], &generation);
        return hdr;
    }

    /// Encode the newly-added rows (`memtable.columns[ci]` from `from..to`)
    /// as an insert record and APPEND BYTES to the file (no fsync). Returns
    /// the post-append cumulative offset; pass to `awaitDurable` once the
    /// caller has released the Table mutex.
    pub fn appendInsert(self: *WalWriter, mt: *const Memtable, from: usize, to: usize) !u64 {
        if (from == to) return self.write_offset;

        var payload: std.ArrayList(u8) = .empty;
        defer payload.deinit(self.allocator);
        try codec.encodeRows(self.allocator, &payload, mt.schema.columns, mt.columns, from, to);
        return self.writeRecord(.insert, payload.items);
    }

    /// Encode one UPDATE batch as a `replace` record (no fsync): the rows it
    /// retracts from the memtable, the offsets it tombstones in segment
    /// `segment_id`, and the replacement rows. Either side may be empty.
    pub fn appendReplace(
        self: *WalWriter,
        schema: []const types.Column,
        retracted: []const ColumnStore,
        retracted_rows: usize,
        segment_id: u64,
        offsets: []const u32,
        inserted: []const ColumnStore,
        inserted_rows: usize,
    ) !u64 {
        var payload: std.ArrayList(u8) = .empty;
        defer payload.deinit(self.allocator);
        try codec.encodeRows(self.allocator, &payload, schema, retracted, 0, retracted_rows);
        try format.appendU64(self.allocator, &payload, segment_id);
        try format.appendU32(self.allocator, &payload, @intCast(offsets.len));
        for (offsets) |offset| try format.appendU32(self.allocator, &payload, offset);
        try codec.encodeRows(self.allocator, &payload, schema, inserted, 0, inserted_rows);
        return self.writeRecord(.replace, payload.items);
    }

    /// Encode a delete predicate as a record (no fsync). Idempotent on replay.
    pub fn appendDelete(self: *WalWriter, pred: anytype) !u64 {
        var payload: std.ArrayList(u8) = .empty;
        defer payload.deinit(self.allocator);

        // col_name
        var b4: [4]u8 = undefined;
        format.writeU32(&b4, @intCast(pred.col.len));
        try payload.appendSlice(self.allocator, &b4);
        try payload.appendSlice(self.allocator, pred.col);

        // op
        try payload.append(self.allocator, @intFromEnum(pred.op));

        // value type tag + bytes
        try payload.append(self.allocator, @intFromEnum(@as(ValueTag, pred.val)));
        try codec.encodeValue(self.allocator, &payload, pred.val);

        return self.writeRecord(.delete, payload.items);
    }

    /// Encode a rich `PredicateExpr` tree (the SQL `DELETE FROM t WHERE
    /// ...` form). Supported variants: leaf / leaf_col_col / is_null /
    /// is_not_null / like / and / or / not / always / in_set. Predicates
    /// containing unresolved subqueries (scalar/exists/in) or var_refs
    /// surface as `error.WalPredicateUnsupported` — caller may proceed
    /// without WAL logging (the delete still executes; durability for
    /// memtable-only state is lost across crash, but segment tombstones
    /// remain durable via their tmp+rename writes).
    ///
    /// `pred` is `anytype` to avoid an engine→exec import cycle —
    /// callers pass an `exec.PredicateExpr`.
    pub fn appendDeleteExpr(self: *WalWriter, pred: anytype) !u64 {
        var payload: std.ArrayList(u8) = .empty;
        defer payload.deinit(self.allocator);
        try codec.encodePredicateExpr(self.allocator, &payload, pred);
        return self.writeRecord(.delete_expr, payload.items);
    }

    pub fn appendFlushMarker(self: *WalWriter, max_segment_id: u64) !u64 {
        var payload: [8]u8 = undefined;
        format.writeU64(&payload, max_segment_id);
        return self.writeRecord(.flush_marker, &payload);
    }

    /// Truncate the WAL back to a fresh file (just the header). Called after
    /// a flush has been durably committed to the manifest. Coordinates with
    /// any in-flight `awaitDurable` so pending waiters don't fsync a file
    /// that's been recreated underneath them.
    pub fn truncate(self: *WalWriter, schema_fingerprint: u64) !void {
        // Drain any in-flight leader fsync before swapping the file out.
        self.coord_mu.lockUncancelable(self.io);
        while (self.in_progress) {
            self.coord_cv.waitUncancelable(self.io, &self.coord_mu);
        }
        self.in_progress = true;
        self.coord_mu.unlock(self.io);
        // A failure below must not leave `in_progress` set: the next flush of
        // this table would park forever on `coord_cv` while holding the table
        // mutex — wedging the table, and (via the core leases every parked
        // statement keeps holding) eventually the whole server.
        errdefer {
            self.coord_mu.lockUncancelable(self.io);
            self.in_progress = false;
            self.coord_cv.broadcast(self.io);
            self.coord_mu.unlock(self.io);
        }

        const temporary = "wal.tmp";
        var generation: [16]u8 = undefined;
        self.io.random(&generation);
        const new_file = try self.dir.createFile(self.io, temporary, .{});
        var owns_new = true;
        errdefer if (owns_new) new_file.close(self.io);
        errdefer self.dir.deleteFile(self.io, temporary) catch {};
        const hdr = makeHeader(schema_fingerprint, generation);
        try new_file.writeStreamingAll(self.io, &hdr);
        try new_file.sync(self.io);
        try Io.Dir.rename(self.dir, temporary, self.dir, wal_filename, self.io);
        self.file.close(self.io);
        self.file = new_file;
        owns_new = false;
        self.generation = generation;
        self.physical_offset = header_size;

        // Anything that was waiting on offsets <= `write_offset` is now
        // implicitly durable (its data is either in a segment or was a delete
        // already applied + segmented out). Bump synced_offset to cover them.
        self.coord_mu.lockUncancelable(self.io);
        self.synced_offset = self.write_offset;
        self.in_progress = false;
        self.coord_cv.broadcast(self.io);
        self.coord_mu.unlock(self.io);
        // The new handle is installed even if directory sync fails. A later
        // writer must never append through the retired, unlinked handle.
        storage.syncDirectory(self.io, self.dir) catch return error.DurabilityUncertain;
    }

    /// Block until the WAL has been durably fsynced through `target_offset`.
    /// Leader-follower group commit with an adaptive coalescing pause:
    ///
    ///   1. If another fsync is already in flight, register as a waiter and
    ///      park on `coord_cv`.
    ///   2. Otherwise become leader. If `waiters > 0` we know other writers
    ///      are queued (and possibly more are mid-append on the way here),
    ///      so pause for `coalesce_pause` before snapshotting. The pause
    ///      lets those in-flight writers bump `write_offset` so this one
    ///      fsync covers them all. When `waiters == 0` we skip the pause
    ///      and fsync immediately (no added latency for single-writer).
    ///   3. Snapshot `write_offset` as late as possible (just before fsync)
    ///      so the snap covers every byte we've observed so far.
    ///   4. Call `file.sync()`, then broadcast.
    pub fn awaitDurable(self: *WalWriter, io: Io, target_offset: u64) !void {
        self.coord_mu.lockUncancelable(io);
        // Count this writer as "in-flight" for the entire lifetime of
        // awaitDurable, not just while parked on the cv. That way, when a
        // newly-claimed leader checks `waiters`, it sees every other writer
        // currently inside awaitDurable — whether they're cv-waiting, racing
        // to claim, or already exiting. We compare against 1 (not 0) since
        // the leader itself is included in the count.
        self.waiters += 1;
        // Defers run in REVERSE order of declaration. We need the decrement
        // to happen FIRST (while holding the mutex), then the unlock — so
        // declare the unlock first (runs last) and the decrement second
        // (runs first).
        defer self.coord_mu.unlock(io);
        defer self.waiters -= 1;

        while (self.synced_offset < target_offset) {
            if (self.in_progress) {
                self.coord_cv.waitUncancelable(io, &self.coord_mu);
                continue;
            }
            self.in_progress = true;
            const initial_waiters = self.waiters;
            self.coord_mu.unlock(io);

            // Group-commit probe: spin briefly to let any in-flight writers
            // arrive at the coordinator. If waiters grow, restart the dwell
            // clock and keep spinning, up to coalesce_max_ns total. This is
            // unconditional (single-writer pays ~20µs extra latency) because
            // the actual signal — others arriving — only shows up DURING the
            // pause; checking before would always see waiters==1.
            var last_seen: u32 = initial_waiters;
            const overall_start = Io.Clock.awake.now(io);
            var dwell_start = overall_start;
            while (true) {
                std.atomic.spinLoopHint();
                const now = Io.Clock.awake.now(io);
                const total_elapsed: u64 = @intCast(overall_start.durationTo(now).toNanoseconds());
                if (total_elapsed >= coalesce_max_ns) break;
                const dwell_elapsed: u64 = @intCast(dwell_start.durationTo(now).toNanoseconds());
                if (dwell_elapsed >= coalesce_probe_ns) {
                    // Probe window elapsed. Check if anyone arrived during it.
                    self.coord_mu.lockUncancelable(io);
                    const current = self.waiters;
                    self.coord_mu.unlock(io);
                    if (current > last_seen) {
                        // Growth detected — extend by restarting dwell clock.
                        last_seen = current;
                        dwell_start = now;
                    } else {
                        break;
                    }
                }
            }
            if (last_seen > initial_waiters) self.coalesce_count += 1;

            self.coord_mu.lockUncancelable(io);
            const snap = self.write_offset;
            self.coord_mu.unlock(io);

            const result = self.file.sync(io);

            self.coord_mu.lockUncancelable(io);
            self.in_progress = false;
            self.fsync_count += 1;
            if (result) |_| {
                if (snap > self.synced_offset) self.synced_offset = snap;
            } else |_| {}
            self.coord_cv.broadcast(io);
            try result;
        }
    }

    /// Block until every record appended so far is durable.
    pub fn awaitAllDurable(self: *WalWriter, io: Io) !void {
        self.coord_mu.lockUncancelable(io);
        const target = self.write_offset;
        self.coord_mu.unlock(io);
        try self.awaitDurable(io, target);
    }

    /// Build the framed bytes (header + payload + checksum), write to file,
    /// and advance `write_offset`. No fsync — durability is established by
    /// a separate `awaitDurable` call after the Table mutex is released.
    fn writeRecord(self: *WalWriter, t: RecordType, payload: []const u8) !u64 {
        const total = record_header_size + payload.len + record_trailer_size;
        const buf = try self.allocator.alloc(u8, total);
        defer self.allocator.free(buf);

        buf[0] = @intFromEnum(t);
        format.writeU32(buf[1..5], @intCast(payload.len));
        @memcpy(buf[5 .. 5 + payload.len], payload);

        const checksum = std.hash.XxHash64.hash(0, buf[0 .. 5 + payload.len]);
        format.writeU64(buf[5 + payload.len ..][0..8], checksum);

        try self.file.writeStreamingAll(self.io, buf);
        self.physical_offset += total;

        self.coord_mu.lockUncancelable(self.io);
        self.write_offset += total;
        const new_offset = self.write_offset;
        self.coord_mu.unlock(self.io);
        return new_offset;
    }
};

pub const ReplayResult = struct { did_replay: bool = false, checkpoint: Checkpoint = .{} };

/// Read the WAL (if any) and apply the records past `checkpoint` and the
/// last flush_marker into `mt`. Segment offsets from `replace` records
/// land in `tombstones`; the caller merges them into the segments.
pub fn replayFromCheckpoint(
    allocator: Allocator,
    io: Io,
    dir: Io.Dir,
    schema_fingerprint: u64,
    mt: *Memtable,
    checkpoint: Checkpoint,
    tombstones: *SegmentTombstones,
) !ReplayResult {
    const bytes = dir.readFileAlloc(io, wal_filename, allocator, .unlimited) catch |err| switch (err) {
        error.FileNotFound => return .{},
        else => return err,
    };
    defer allocator.free(bytes);

    if (bytes.len < legacy_header_size) return Error.WalTooSmall;
    if (!std.mem.eql(u8, bytes[0..4], &wal_magic)) return Error.WalBadMagic;
    const version = format.readU16(bytes[4..6]);
    if (version != 1 and version != wal_version) return Error.WalUnsupportedVersion;
    const actual_header_size: usize = if (version == 1) legacy_header_size else header_size;
    if (bytes.len < actual_header_size) return Error.WalTooSmall;
    const generation: [16]u8 = if (version == 1) @splat(0) else bytes[16..32].*;
    const fp = format.readU64(bytes[8..16]);
    if (fp != schema_fingerprint) return Error.WalSchemaFingerprintMismatch;

    // First pass: find the position immediately after the last flush_marker.
    var replay_start: usize = actual_header_size;
    if (checkpoint.offset != 0 and std.mem.eql(u8, &generation, &checkpoint.generation)) {
        if (checkpoint.offset < actual_header_size or checkpoint.offset > bytes.len) return Error.WalCorrupt;
        replay_start = @intCast(checkpoint.offset);
    }
    var cursor: usize = replay_start;
    while (cursor < bytes.len) {
        const next = readRecord(bytes, cursor) catch |err| switch (err) {
            // Truncated tail (partial write before crash) — stop here.
            Error.WalCorrupt, Error.WalTooSmall => break,
            else => return err,
        };
        if (next.type == .flush_marker) replay_start = next.cursor_after;
        cursor = next.cursor_after;
    }

    // Second pass: replay everything after `replay_start`.
    var did_replay: bool = false;
    cursor = replay_start;
    while (cursor < bytes.len) {
        const rec = readRecord(bytes, cursor) catch |err| switch (err) {
            Error.WalCorrupt, Error.WalTooSmall => break,
            else => return err,
        };
        switch (rec.type) {
            .insert => try codec.applyInsertRecord(allocator, rec.payload, mt),
            .delete => try codec.applyDeleteRecord(allocator, rec.payload, mt),
            .delete_expr => try codec.applyDeleteExprRecord(allocator, rec.payload, mt),
            .replace => try codec.applyReplaceRecord(allocator, rec.payload, mt, tombstones),
            .flush_marker => {},
        }
        did_replay = true;
        cursor = rec.cursor_after;
    }

    return .{ .did_replay = did_replay, .checkpoint = .{ .generation = generation, .offset = cursor } };
}

const ReadRecord = struct {
    type: RecordType,
    payload: []const u8,
    cursor_after: usize,
};

fn readRecord(bytes: []const u8, off: usize) !ReadRecord {
    if (off + record_header_size > bytes.len) return Error.WalTooSmall;
    const tag_byte = bytes[off];
    if (tag_byte < 1 or tag_byte > @intFromEnum(RecordType.replace)) return Error.WalUnknownRecord;
    const t: RecordType = @enumFromInt(tag_byte);
    const payload_len = format.readU32(bytes[off + 1 .. off + 5]);
    const payload_end = off + record_header_size + payload_len;
    if (payload_end + record_trailer_size > bytes.len) return Error.WalCorrupt;
    const checksum_stored = format.readU64(bytes[payload_end .. payload_end + 8]);
    const checksum_actual = std.hash.XxHash64.hash(0, bytes[off..payload_end]);
    if (checksum_stored != checksum_actual) return Error.WalCorrupt;
    return .{
        .type = t,
        .payload = bytes[off + record_header_size .. payload_end],
        .cursor_after = payload_end + record_trailer_size,
    };
}

test "wal v1 records remain readable without a generation" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const schema = types.TableSchema{
        .columns = &.{.{ .name = "id", .type = .bigint }},
        .order_key = &.{"id"},
        .unique = false,
    };
    var source = try Memtable.init(a, schema);
    defer source.deinit();
    try source.appendValue(0, i64, 42);
    source.row_count = 1;
    {
        var writer = try WalWriter.create(a, io, tmp.dir, 1234);
        defer writer.deinit();
        _ = try writer.appendInsert(&source, 0, 1);
    }
    const current = try tmp.dir.readFileAlloc(io, wal_filename, a, .unlimited);
    defer a.free(current);
    const legacy = try a.alloc(u8, current.len - (header_size - legacy_header_size));
    defer a.free(legacy);
    @memcpy(legacy[0..legacy_header_size], current[0..legacy_header_size]);
    @memcpy(legacy[legacy_header_size..], current[header_size..]);
    std.mem.writeInt(u16, legacy[4..6], 1, .little);
    try tmp.dir.writeFile(io, .{ .sub_path = wal_filename, .data = legacy });
    var recovered = try Memtable.init(a, schema);
    defer recovered.deinit();
    var tombstones: SegmentTombstones = .empty;
    defer deinitSegmentTombstones(a, &tombstones);
    const result = try replayFromCheckpoint(a, io, tmp.dir, 1234, &recovered, .{}, &tombstones);
    try std.testing.expect(result.did_replay);
    try std.testing.expectEqualSlices(i64, &.{42}, recovered.columns[0].data.bigint.items);
    recovered.clear();
    const again = try replayFromCheckpoint(a, io, tmp.dir, 1234, &recovered, result.checkpoint, &tombstones);
    try std.testing.expect(!again.did_replay);
    try std.testing.expectEqual(@as(u64, 0), recovered.row_count);
}

test "wal replace records retract one equal row each and hand back their offsets" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const schema = types.TableSchema{
        .columns = &.{ .{ .name = "id", .type = .bigint }, .{ .name = "tag", .type = .string } },
        .order_key = &.{"id"},
        .unique = false,
    };
    var live = try Memtable.init(a, schema);
    defer live.deinit();
    try live.insertRows(&.{
        .{ .id = @as(i64, 1), .tag = "a" },
        .{ .id = @as(i64, 2), .tag = "b" },
        .{ .id = @as(i64, 2), .tag = "b" },
        .{ .id = @as(i64, 3), .tag = "c" },
    });
    var retracted = try Memtable.init(a, schema);
    defer retracted.deinit();
    try retracted.insertRows(&.{.{ .id = @as(i64, 2), .tag = "b" }});
    var inserted = try Memtable.init(a, schema);
    defer inserted.deinit();
    try inserted.insertRows(&.{.{ .id = @as(i64, 2), .tag = "z" }});
    {
        var writer = try WalWriter.create(a, io, tmp.dir, 99);
        defer writer.deinit();
        _ = try writer.appendInsert(&live, 0, 4);
        _ = try writer.appendReplace(schema.columns, retracted.columns, 1, 7, &.{ 4, 1 }, inserted.columns, 1);
        _ = try writer.appendReplace(schema.columns, &.{}, 0, 8, &.{0}, &.{}, 0);
    }

    var recovered = try Memtable.init(a, schema);
    defer recovered.deinit();
    var tombstones: SegmentTombstones = .empty;
    defer deinitSegmentTombstones(a, &tombstones);
    _ = try replayFromCheckpoint(a, io, tmp.dir, 99, &recovered, .{}, &tombstones);

    try std.testing.expectEqualSlices(i64, &.{ 1, 2, 3, 2 }, recovered.columns[0].data.bigint.items);
    const tags = recovered.columns[1].view().data.string;
    for ([_][]const u8{ "a", "b", "c", "z" }, 0..) |want, row| {
        try std.testing.expectEqualStrings(want, tags.rowBytes(row));
    }
    try std.testing.expectEqual(@as(usize, 2), tombstones.count());
    try std.testing.expectEqualSlices(u32, &.{ 4, 1 }, tombstones.get(7).?.items);
    try std.testing.expectEqualSlices(u32, &.{0}, tombstones.get(8).?.items);
}

test "truncate failure clears the group-commit coordinator" {
    // Windows refuses to delete a directory with open handles, which is the
    // fault lever this test uses; the code under test is platform-neutral.
    if (builtin.os.tag == .windows) return error.SkipZigTest;
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var wal_dir = try tmp.dir.createDirPathOpen(io, "wal_dir", .{});
    defer wal_dir.close(io);
    var w = try WalWriter.create(allocator, io, wal_dir, 0xabc);
    defer w.deinit();

    // Delete the directory out from under the writer so truncate's
    // createFile fails mid-operation.
    try tmp.dir.deleteTree(io, "wal_dir");

    try std.testing.expectError(error.FileNotFound, w.truncate(0xabc));
    // The regression: a failed truncate must not leave `in_progress` set —
    // a poisoned flag makes the NEXT flush park forever holding the table
    // mutex. A retry must error again immediately, never hang.
    try std.testing.expect(!w.in_progress);
    try std.testing.expectError(error.FileNotFound, w.truncate(0xabc));
    try std.testing.expect(!w.in_progress);
}
