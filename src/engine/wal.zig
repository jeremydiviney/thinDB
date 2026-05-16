//! Write-ahead log. Every insert and delete is appended to a single
//! append-only file BEFORE the operation is applied to the memtable.
//! The fsync after that append is the durability point: once `insert`
//! returns success, the data survives a process / power crash.
//!
//! On reopen, the WAL is replayed to reconstruct the in-memory state
//! that was lost when the previous process exited (with whatever was
//! still in the memtable).
//!
//! File layout (binary, little-endian):
//!
//!   Header (16 bytes):
//!     magic "tDBW"        4
//!     version u16         2
//!     flags u16           2  (reserved, 0)
//!     schema_fingerprint  8  (must match table's schema fingerprint on open)
//!
//!   Sequence of records, each:
//!     type u8             1   (1=insert, 2=delete, 3=flush_marker)
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

const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Type = types.Type;
const Schema = types.Schema;
const TypeTag = types.TypeTag;
const ValueTag = types.ValueTag;
const Value = types.Value;

const storage = @import("../storage/storage.zig");
const format = storage.format;
const ColumnView = storage.ColumnView;
const column_mod = storage.column;

const store = @import("store.zig");
const ColumnStore = store.ColumnStore;

const memtable_mod = @import("memtable.zig");
const Memtable = memtable_mod.Memtable;

const codec = @import("wal_codec.zig");

pub const wal_magic: [4]u8 = .{ 't', 'D', 'B', 'W' };
pub const wal_version: u16 = 1;
pub const wal_filename = "wal";
pub const header_size: usize = 16;
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
};

pub const Error = error{
    WalBadMagic,
    WalUnsupportedVersion,
    WalSchemaFingerprintMismatch,
    WalCorrupt,
    WalUnknownRecord,
    WalTooSmall,
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

        var hdr: [header_size]u8 = undefined;
        @memcpy(hdr[0..4], &wal_magic);
        format.writeU16(hdr[4..6], wal_version);
        format.writeU16(hdr[6..8], 0);
        format.writeU64(hdr[8..16], schema_fingerprint);
        try file.writeStreamingAll(io, &hdr);
        try file.sync(io);

        return .{
            .allocator = allocator,
            .io = io,
            .dir = dir,
            .file = file,
            .write_offset = header_size,
            .synced_offset = header_size,
        };
    }

    pub fn deinit(self: *WalWriter) void {
        self.file.close(self.io);
        self.* = undefined;
    }

    /// Encode the newly-added rows (`memtable.columns[ci]` from `from..to`)
    /// as an insert record and APPEND BYTES to the file (no fsync). Returns
    /// the post-append cumulative offset; pass to `awaitDurable` once the
    /// caller has released the Table mutex.
    pub fn appendInsert(self: *WalWriter, mt: *const Memtable, from: usize, to: usize) !u64 {
        if (from == to) return self.write_offset;

        var payload: std.ArrayList(u8) = .empty;
        defer payload.deinit(self.allocator);

        // row_count u32
        var b4: [4]u8 = undefined;
        format.writeU32(&b4, @intCast(to - from));
        try payload.appendSlice(self.allocator, &b4);

        // per-column encoding in schema order
        for (mt.columns, mt.schema.columns) |col, schema_col| {
            try codec.encodeColumnRange(self.allocator, &payload, col, schema_col, from, to);
        }

        return self.writeRecord(.insert, payload.items);
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

        // Close, recreate (truncates), write header, sync.
        self.file.close(self.io);
        self.file = try self.dir.createFile(self.io, wal_filename, .{});

        var hdr: [header_size]u8 = undefined;
        @memcpy(hdr[0..4], &wal_magic);
        format.writeU16(hdr[4..6], wal_version);
        format.writeU16(hdr[6..8], 0);
        format.writeU64(hdr[8..16], schema_fingerprint);
        try self.file.writeStreamingAll(self.io, &hdr);
        try self.file.sync(self.io);

        // Anything that was waiting on offsets <= `write_offset` is now
        // implicitly durable (its data is either in a segment or was a delete
        // already applied + segmented out). Bump synced_offset to cover them.
        self.coord_mu.lockUncancelable(self.io);
        self.synced_offset = self.write_offset;
        self.in_progress = false;
        self.coord_cv.broadcast(self.io);
        self.coord_mu.unlock(self.io);
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

        self.coord_mu.lockUncancelable(self.io);
        self.write_offset += total;
        const new_offset = self.write_offset;
        self.coord_mu.unlock(self.io);
        return new_offset;
    }
};

/// Read the WAL (if any) and apply records since the last flush_marker into
/// `mt`. Caller passes the table's schema_fingerprint for validation.
/// Returns `true` if any records were replayed.
pub fn replay(
    allocator: Allocator,
    io: Io,
    dir: Io.Dir,
    schema_fingerprint: u64,
    mt: *Memtable,
) !bool {
    const bytes = dir.readFileAlloc(io, wal_filename, allocator, .unlimited) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };
    defer allocator.free(bytes);

    if (bytes.len < header_size) return Error.WalTooSmall;
    if (!std.mem.eql(u8, bytes[0..4], &wal_magic)) return Error.WalBadMagic;
    const version = format.readU16(bytes[4..6]);
    if (version != wal_version) return Error.WalUnsupportedVersion;
    const fp = format.readU64(bytes[8..16]);
    if (fp != schema_fingerprint) return Error.WalSchemaFingerprintMismatch;

    // First pass: find the position immediately after the last flush_marker.
    var cursor: usize = header_size;
    var replay_start: usize = header_size;
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
            .flush_marker => {},
        }
        did_replay = true;
        cursor = rec.cursor_after;
    }

    return did_replay;
}

const ReadRecord = struct {
    type: RecordType,
    payload: []const u8,
    cursor_after: usize,
};

fn readRecord(bytes: []const u8, off: usize) !ReadRecord {
    if (off + record_header_size > bytes.len) return Error.WalTooSmall;
    const tag_byte = bytes[off];
    if (tag_byte < 1 or tag_byte > 3) return Error.WalUnknownRecord;
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

