//! CTE / FROM-subquery stage materialization (engine V2).
//!
//! Every CTE and FROM-subquery boundary materializes: the stage's pipeline
//! drains ONCE into a chunked columnar `MaterializedResult`, and each
//! downstream reference scans it through a `MatScan` leaf — a fresh query
//! pipeline per stage, chained stage-to-stage. Semantics:
//!
//!   - shared stage (MATERIALIZED / default): all references read one result
//!   - regenerated stage (NOT MATERIALIZED): each reference owns its own
//!     Stage over the same IR subtree, recomputed per use
//!
//! Execution is lazy and demand-driven: a stage runs the first time one of
//! its consumers calls `next()`, which recursively runs its own upstream
//! stages the same way. All `next()`/teardown calls happen on the single
//! connection thread driving the root query, so stage state needs no locks.
//!
//! The result frees the moment its LAST consumer finishes draining it — on a
//! background thread so the teardown overlaps the next stage's work — and
//! the thread is joined at StageSet teardown so memory accounting stays
//! deterministic (leak-checked tests included).
//!
//! Chunks are sized for striping (`chunk_rows` ≈ one row group): a future
//! parallel consumer claims chunk ranges exactly like segment row-group
//! tiles. Today each stage has one serial MatScan reader; the parallelism
//! lives inside table-sourced stages (which run the regular V2 handlers).

const std = @import("std");
const Allocator = std.mem.Allocator;

const exec = @import("exec.zig");
const window_mod = @import("window.zig");
const table_fn_mod = @import("table_fn.zig");
const join_mod = @import("join.zig");
const project_limit = @import("project_limit.zig");
const types = @import("../types.zig");
const engine = @import("../engine/engine.zig");
const storage = @import("../storage/storage.zig");

const Column = types.Column;
const ColumnView = storage.ColumnView;

/// A/B escape hatch: `THINDB_MAT_PERROW` forces the legacy per-cell
/// `appendOneRow` materialization loop so the bulk column-major path can be
/// benchmarked against it in a single running server. Default (unset) = bulk.
extern "c" fn getenv(name: [*:0]const u8) ?[*:0]const u8;

/// Rows per chunk — mirrors the default segment row-group size so chunk
/// striping behaves like row-group tiling for future parallel readers.
pub const chunk_rows: usize = 65_536;

pub const MaterializedResult = struct {
    allocator: Allocator,
    /// Borrowed from the owning Stage's schema copy.
    schema: []const Column,
    chunks: std.ArrayListUnmanaged(Chunk) = .empty,
    total_rows: u64 = 0,
    /// Provable upper bound on the final row count (the stage's compile-time
    /// `stats_upper_rows`), used to size a fresh chunk's columns exactly: a
    /// small stage allocates a small chunk instead of a full `chunk_rows`.
    expected_rows: u64 = 0,
    /// Set when the result was adopted zero-copy from a producer's own
    /// contiguous buffers (window-output-as-stage) instead of pull-copied.
    /// The chunks are then borrowed view windows over these stores.
    adopted: ?Adopted = null,
    /// The slice-key column name when this result was slice-adopted (chunks
    /// carry key ranges); readers validate their skip column against it.
    sliced_key: ?[]const u8 = null,
    /// Hash-join builds over this result, one per distinct key set, shared
    /// by every join that binds the stage as its build side. Freed with
    /// the chunks they view.
    join_builds: std.ArrayListUnmanaged(SharedJoinBuild) = .empty,

    /// Ownership record for adopted contiguous columns: entries flagged in
    /// `arena_backed` are reclaimed wholesale by sweeping `arenas`; the
    /// rest deinit with `store_alloc` when set (a producer that allocated
    /// them elsewhere — parallel-scan worker buffers), else the result's
    /// allocator.
    pub const Adopted = struct {
        stores: []engine.ColumnStore,
        arenas: []std.heap.ArenaAllocator,
        arena_backed: []bool,
        store_alloc: ?Allocator = null,
    };

    pub const Chunk = struct {
        /// Owned column storage (the pull-copy path). Empty for adopted
        /// results — readers consume `views`.
        cols: []engine.ColumnStore = &.{},
        /// Borrowed view windows over adopted contiguous stores. Chunk
        /// bounds land on 64K-row (byte-aligned) offsets, so the validity
        /// bitmaps slice cleanly.
        views: []ColumnView = &.{},
        rows: usize = 0,
        /// Keyed-region exit graft (slice-adopted results): this chunk's
        /// slice-key range — values are in (key_lo, key_hi], null bound =
        /// unbounded. A slice-range reader skips chunks with disjoint
        /// ranges (see MatScan.setSliceSkip). Defaults are the "no
        /// information" state: never skipped.
        key_lo: ?types.Value = null,
        key_hi: ?types.Value = null,
        key_nulls: bool = true,
    };

    /// Take ownership of a producer's contiguous result columns and expose
    /// them as `chunk_rows`-sized view chunks — same shape the parallel
    /// buffer scan partitions, no copy.
    pub fn adoptContiguous(self: *MaterializedResult, adopted: Adopted, rows: u64) !void {
        self.adopted = adopted;
        const n: usize = @intCast(rows);
        var lo: usize = 0;
        while (lo < n) {
            const take = @min(chunk_rows, n - lo);
            const views = try self.allocator.alloc(ColumnView, adopted.stores.len);
            errdefer self.allocator.free(views);
            for (adopted.stores, self.schema, views) |*st, sc, *v| {
                v.* = presentAsSchemaType(engine.transform.subViewAligned(st.view(), lo, take), sc.type);
            }
            try self.chunks.append(self.allocator, .{ .views = views, .rows = take });
            lo += take;
        }
        self.total_rows = rows;
    }

    /// Keyed-region exit graft: adopt N slice sinks' contiguous stores as
    /// this result's chunks, in slice order — a deterministic concat
    /// (disjoint key ranges in ascending slice order also preserve a
    /// leading-slice-key sort). Takes full ownership: the sinks' store/arena
    /// arrays are copied into one flat Adopted record and their top-level
    /// arrays freed; the arena-backed column data frees with the result's
    /// normal adopted sweep.
    pub const SliceKey = struct {
        col: []const u8,
        /// Ascending slice boundaries: slice i holds (bounds[i-1], bounds[i]],
        /// open at the ends; NULL keys live in slice 0. Borrowed — must
        /// outlive the result (the sliced-fill ctx owns them).
        bounds: []const types.Value,
    };

    pub fn adoptSlices(self: *MaterializedResult, sinks: []ContigSink, key: ?SliceKey) !void {
        const ncols = self.schema.len;
        const stores = try self.allocator.alloc(engine.ColumnStore, sinks.len * ncols);
        errdefer self.allocator.free(stores);
        const arenas = try self.allocator.alloc(std.heap.ArenaAllocator, sinks.len * ncols);
        errdefer self.allocator.free(arenas);
        const backed = try self.allocator.alloc(bool, sinks.len * ncols);
        errdefer self.allocator.free(backed);
        for (sinks, 0..) |*sk, i| {
            const ad = sk.take();
            @memcpy(stores[i * ncols ..][0..ncols], ad.stores);
            @memcpy(arenas[i * ncols ..][0..ncols], ad.arenas);
            @memcpy(backed[i * ncols ..][0..ncols], ad.arena_backed);
            sk.allocator.free(ad.stores);
            sk.allocator.free(ad.arenas);
            sk.allocator.free(ad.arena_backed);
        }
        self.adopted = .{ .stores = stores, .arenas = arenas, .arena_backed = backed };
        if (key) |k| self.sliced_key = k.col;
        for (sinks, 0..) |*sk, i| {
            const pstores = stores[i * ncols ..][0..ncols];
            const n: usize = @intCast(sk.rows);
            var lo: usize = 0;
            while (lo < n) {
                const take_n = @min(chunk_rows, n - lo);
                const views = try self.allocator.alloc(ColumnView, ncols);
                errdefer self.allocator.free(views);
                for (pstores, self.schema, views) |*st, sc, *v| {
                    v.* = presentAsSchemaType(engine.transform.subViewAligned(st.view(), lo, take_n), sc.type);
                }
                var chunk: Chunk = .{ .views = views, .rows = take_n };
                if (key) |k| {
                    chunk.key_lo = if (i > 0) k.bounds[i - 1] else null;
                    chunk.key_hi = if (i < k.bounds.len) k.bounds[i] else null;
                    chunk.key_nulls = i == 0;
                }
                try self.chunks.append(self.allocator, chunk);
                lo += take_n;
            }
            self.total_rows += sk.rows;
        }
    }

    /// Adopt a producer's per-chunk owned stores (parallel-scan worker
    /// buffers via `takeOwnedChunks`) as this result's chunks — the pull-copy
    /// path's second deep copy of the whole result disappears. The chunks'
    /// stores flatten into the single `Adopted` record (owned by `oc.alloc`);
    /// view chunks are cut per owned chunk in ≤`chunk_rows` windows (64K
    /// offsets keep validity bitmaps byte-aligned). Takes full ownership,
    /// including of the handle's arrays.
    pub fn adoptOwnedChunks(self: *MaterializedResult, oc: exec.OwnedChunks) !void {
        // The handle owns the store buffers until the infallible tail below —
        // every fallible step frees ONLY its own arrays on error (view chunks
        // already appended sweep with the result's normal deinitChunks; their
        // `cols` are empty, so nothing double-frees the shared buffers).
        errdefer exec.deinitOwnedChunks(oc);
        const ncols = self.schema.len;
        for (oc.chunks) |c| {
            if (c.stores.len != ncols) return exec.Error.TypeMismatch;
        }
        const stores = try self.allocator.alloc(engine.ColumnStore, oc.chunks.len * ncols);
        errdefer self.allocator.free(stores);
        const backed = try self.allocator.alloc(bool, oc.chunks.len * ncols);
        errdefer self.allocator.free(backed);
        @memset(backed, false);
        const arenas = try self.allocator.alloc(std.heap.ArenaAllocator, 0);
        errdefer self.allocator.free(arenas);

        var added: u64 = 0;
        for (oc.chunks, 0..) |c, i| {
            @memcpy(stores[i * ncols ..][0..ncols], c.stores);
            const pstores = stores[i * ncols ..][0..ncols];
            var lo: usize = 0;
            while (lo < c.rows) {
                const take = @min(chunk_rows, c.rows - lo);
                const views = try self.allocator.alloc(ColumnView, ncols);
                errdefer self.allocator.free(views);
                for (pstores, self.schema, views) |*st, sc, *v| {
                    v.* = presentAsSchemaType(engine.transform.subViewAligned(st.view(), lo, take), sc.type);
                }
                try self.chunks.append(self.allocator, .{ .views = views, .rows = take });
                lo += take;
            }
            added += c.rows;
        }

        // Infallible tail: buffer ownership moves to `adopted`, the handle's
        // arrays free, and the row count commits.
        self.adopted = .{ .stores = stores, .arenas = arenas, .arena_backed = backed, .store_alloc = oc.alloc };
        for (oc.chunks) |c| oc.alloc.free(c.stores);
        oc.alloc.free(oc.chunks);
        self.total_rows += added;
    }

    /// The pull-copy path launders physical string-family tags into the
    /// stage schema's tag (chunk stores are schema-typed); zero-copy
    /// adoption must present the same tags or consumers compiled against
    /// the schema hit the wrong union arm. varchar/string/char share one
    /// physical layout (offsets + bytes), so the retag is free. Pub: the
    /// TVF input borrow re-tags borrowed store views the same way.
    pub fn presentAsSchemaType(v: ColumnView, t: types.Type) ColumnView {
        const sv = switch (v.data) {
            .varchar, .string, .char, .json => |x| x,
            else => return v,
        };
        return switch (t) {
            .varchar => .{ .data = .{ .varchar = sv }, .nulls = v.nulls },
            .string => .{ .data = .{ .string = sv }, .nulls = v.nulls },
            .char => .{ .data = .{ .char = sv }, .nulls = v.nulls },
            else => v,
        };
    }

    fn appendBatch(self: *MaterializedResult, batch: exec.Batch) !void {
        const per_row = getenv("THINDB_MAT_PERROW") != null;
        var row: usize = 0;
        while (row < batch.row_count) {
            const chunk = try self.openChunk();
            const take = @min(chunk_rows - chunk.rows, batch.row_count - row);
            if (per_row) {
                var r = row;
                while (r < row + take) : (r += 1) {
                    for (chunk.cols, 0..) |*dst, ci| {
                        try engine.transform.appendOneRow(self.allocator, batch.values[ci], r, dst);
                    }
                }
            } else {
                for (chunk.cols, 0..) |*dst, ci| {
                    try engine.transform.appendColumnRange(self.allocator, batch.values[ci], row, row + take, dst);
                }
            }
            chunk.rows += take;
            self.total_rows += take;
            row += take;
        }
    }

    /// The last chunk if it has room, else a fresh one — with every row-counted
    /// buffer (fixed-width data, string offsets, validity bitmap) pre-sized to
    /// `cap_rows` so the fill loop never reallocates them. `cap_rows` is exact:
    /// the provable row-count bound caps it below `chunk_rows` for small stages.
    /// A string column's variable-width byte buffer is reserved from the
    /// PREVIOUS full chunk's exact bytes/row (+10% slack) — a strong, locally
    /// adaptive predictor that tracks clustered data without the global-average
    /// variance that would under-reserve a dense chunk into a geometric realloc
    /// that then stays bloated. The first chunk has no predecessor, so its byte
    /// buffer grows naturally (a single chunk's worth of reallocs).
    fn openChunk(self: *MaterializedResult) !*Chunk {
        const prev: ?*Chunk = if (self.chunks.items.len > 0) &self.chunks.items[self.chunks.items.len - 1] else null;
        if (prev) |last| {
            if (last.rows < chunk_rows) return last;
        }
        const remaining: u64 = if (self.expected_rows > self.total_rows) self.expected_rows - self.total_rows else 0;
        const cap_rows: usize = if (remaining == 0) chunk_rows else @intCast(@min(@as(u64, chunk_rows), remaining));
        const cols = try self.allocator.alloc(engine.ColumnStore, self.schema.len);
        var inited: usize = 0;
        errdefer {
            for (cols[0..inited]) |*c| c.deinit(self.allocator);
            self.allocator.free(cols);
        }
        for (self.schema, 0..) |sc, i| {
            const bytes_cap: usize = switch (sc.type) {
                .varchar, .string, .char, .json => blk: {
                    const p = prev orelse break :blk 0;
                    if (p.rows == 0) break :blk 0;
                    const bpr = (colStrBytes(&p.cols[i]) + p.rows - 1) / p.rows;
                    break :blk ((bpr * cap_rows) * 11) / 10;
                },
                else => 0,
            };
            cols[i] = try engine.ColumnStore.initCapacity(self.allocator, sc.type, sc.nullable, cap_rows, bytes_cap);
            inited += 1;
        }
        try self.chunks.append(self.allocator, .{ .cols = cols });
        return &self.chunks.items[self.chunks.items.len - 1];
    }

    /// Bytes held by a string-family column store; 0 for fixed-width.
    fn colStrBytes(col: *const engine.ColumnStore) usize {
        return switch (col.data) {
            .varchar, .string, .char, .json => |ss| ss.bytes.items.len,
            else => 0,
        };
    }

    /// One view per column spanning every row, presented as the schema
    /// types. Adopted results and single-chunk pull copies are contiguous
    /// already; anything else is concatenated once into `copies`
    /// (owned by the result's allocator, freed by the caller).
    fn contiguousViews(self: *MaterializedResult, views: []ColumnView, copies: *[]engine.ColumnStore) !void {
        if (self.adopted) |ad| {
            if (ad.stores.len == self.schema.len) {
                for (ad.stores, self.schema, views) |*st, sc, *v| v.* = presentAsSchemaType(st.view(), sc.type);
                return;
            }
        } else if (self.chunks.items.len == 1 and self.chunks.items[0].cols.len == self.schema.len) {
            for (self.chunks.items[0].cols, views) |*col, *v| v.* = col.view();
            return;
        }
        const stores = try self.allocator.alloc(engine.ColumnStore, self.schema.len);
        errdefer self.allocator.free(stores);
        var inited: usize = 0;
        errdefer for (stores[0..inited]) |*st| st.deinit(self.allocator);
        for (self.schema, stores) |sc, *st| {
            st.* = try engine.ColumnStore.init(self.allocator, sc.type, sc.nullable);
            inited += 1;
        }
        for (self.chunks.items) |c| {
            if (c.rows == 0) continue;
            for (stores, 0..) |*st, i| {
                const src = if (c.views.len > 0) c.views[i] else c.cols[i].view();
                try engine.transform.appendAllColumn(self.allocator, src, st);
            }
        }
        for (stores, views) |*st, *v| v.* = st.view();
        copies.* = stores;
    }

    fn deinitChunks(self: *MaterializedResult) void {
        for (self.join_builds.items) |*jb| jb.deinit(self.allocator);
        self.join_builds.deinit(self.allocator);
        for (self.chunks.items) |*c| {
            for (c.cols) |*col| col.deinit(self.allocator);
            if (c.cols.len > 0) self.allocator.free(c.cols);
            if (c.views.len > 0) self.allocator.free(c.views);
        }
        self.chunks.deinit(self.allocator);
        if (self.adopted) |*ad| {
            for (ad.stores, ad.arena_backed) |*st, ab| {
                if (!ab) st.deinit(ad.store_alloc orelse self.allocator);
            }
            for (ad.arenas) |*a| a.deinit();
            self.allocator.free(ad.stores);
            self.allocator.free(ad.arenas);
            self.allocator.free(ad.arena_backed);
            self.adopted = null;
        }
    }
};

/// A hash-join build over a stage result: contiguous column views plus
/// the FastTable for one key set. Built once by the first join that
/// binds the stage with these keys; later joins with the same keys reuse
/// it (`Stage.sharedJoinBuild`).
pub const SharedJoinBuild = struct {
    keys: []const KeySpec,
    casts: []const ColumnCast,
    needs_chain: bool,
    arena: std.heap.ArenaAllocator,
    /// Concatenated copy of a chunked result; empty when the result was
    /// contiguous and the views borrow it directly.
    copies: []engine.ColumnStore,
    /// One store per entry of `casts`, holding that cast's full-column output.
    cast_stores: []engine.ColumnStore,
    views: []ColumnView,
    rows: u32,
    table: join_mod.FastTable,
    keys_unique: bool,

    fn matches(self: *const SharedJoinBuild, keys: []const KeySpec, casts: []const ColumnCast, needs_chain: bool) bool {
        if (self.needs_chain != needs_chain or self.keys.len != keys.len or self.casts.len != casts.len) return false;
        for (self.keys, keys) |a, b| if (!a.eql(b)) return false;
        for (self.casts, casts) |a, b| if (!a.eql(b)) return false;
        return true;
    }

    /// The view a slot reading stage column `col` under `cast` sees. Every
    /// cast a consumer asks for is in `casts` (it matched or built this).
    pub fn viewFor(self: *const SharedJoinBuild, col: usize, cast: ?[]const u8) ColumnView {
        const fn_name = cast orelse return self.views[col];
        for (self.casts, self.cast_stores) |c, *st| {
            if (c.col == col and std.ascii.eqlIgnoreCase(c.fn_name, fn_name)) return st.view();
        }
        unreachable;
    }

    fn deinit(self: *SharedJoinBuild, allocator: Allocator) void {
        for (self.copies) |*c| c.deinit(allocator);
        if (self.copies.len > 0) allocator.free(self.copies);
        for (self.cast_stores) |*c| c.deinit(allocator);
        if (self.cast_stores.len > 0) allocator.free(self.cast_stores);
        self.arena.deinit();
    }
};

/// One join key over a stage: the stage column and the same-named cast
/// (a `to_*` function name) the build side applies to it, if any.
pub const KeySpec = struct {
    col: usize,
    cast: ?[]const u8,

    pub fn eql(a: KeySpec, b: KeySpec) bool {
        if (a.col != b.col) return false;
        const ac = a.cast orelse return b.cast == null;
        const bc = b.cast orelse return false;
        return std.ascii.eqlIgnoreCase(ac, bc);
    }
};

/// A same-named single-column cast a Compute lays over the stage's
/// output: slot `col` (a stage column index) reads `fn_name` of that
/// column. Join-key type coercion puts one above a build side.
pub const ColumnCast = struct {
    col: usize,
    fn_name: []const u8,

    pub fn eql(a: ColumnCast, b: ColumnCast) bool {
        return a.col == b.col and std.ascii.eqlIgnoreCase(a.fn_name, b.fn_name);
    }
};

pub const MAX_STAGE_CASTS: usize = 8;

/// Evaluate `fn_name(src)` over every row of one stage column through the
/// overload a Compute would pick, into a store owned by `allocator`. Null
/// when the overload isn't a plain null-propagating kernel (the join then
/// keeps its own build).
fn castStageColumn(
    allocator: Allocator,
    aa: Allocator,
    fn_name: []const u8,
    src: ColumnView,
    src_type: types.Type,
    rows: usize,
) !?engine.ColumnStore {
    const ov = (try exec.scalar_fn.resolve(aa, fn_name, &.{src_type})) orelse return null;
    if (ov.func.null_strategy != .propagates or ov.func.udf_kernel != null) return null;
    var arg = src;
    var arg_buf: ?engine.ColumnStore = null;
    defer if (arg_buf) |*b| b.deinit(allocator);
    if (ov.arg_casts) |ac| {
        if (ac[0]) |k| {
            arg_buf = try engine.ColumnStore.init(allocator, ov.func.arg_types[0], true);
            var one = [_]ColumnView{src};
            try k(allocator, &one, &arg_buf.?, rows);
            arg = arg_buf.?.view();
        }
    }
    var out = try engine.ColumnStore.init(allocator, ov.func.return_type, true);
    errdefer out.deinit(allocator);
    var args = [_]ColumnView{arg};
    if (ov.func.typed_kernel) |tk| {
        try tk(allocator, &.{src_type}, ov.func.return_type, &args, &out, rows);
    } else if (ov.func.kernel) |k| {
        try k(allocator, &args, &out, rows);
    } else return null;
    try out.appendValidityRange(allocator, 0, arg.nulls, rows);
    return out;
}

/// The stage a query reads, seen through its wrappers, with what each of
/// the query's output slots reads from it.
pub const StageSource = struct {
    stage: *Stage,
    /// Per output slot: the stage column it reads.
    map: []usize,
    /// Per output slot: the same-named `to_*` cast applied over it, or null.
    casts: []?[]const u8,

    pub fn deinit(self: *StageSource, allocator: Allocator) void {
        allocator.free(self.map);
        allocator.free(self.casts);
    }
};

/// Peel `q_in` down to the stage it reads through AliasRename, a column
/// Project and Computes that only rename (a `col_ref` derived — the
/// parser's hidden `__join_on_*` key columns) or cast a slot in place
/// (`to_*(slot)` — join-key type coercion). Null for anything else, a
/// probe-fused wrapper, or a MatScan with a slice-skip hint (it doesn't
/// read every chunk).
/// `reason`, when given, names the operator that stopped the peel on a null
/// return (join-fusion trace).
pub fn stageBehind(allocator: Allocator, q_in: exec.Query, reason: ?*[]const u8) !?StageSource {
    const n = q_in.outputSchema().len;
    const map = try allocator.alloc(usize, n);
    errdefer allocator.free(map);
    for (map, 0..) |*m, i| m.* = i;
    const casts = try allocator.alloc(?[]const u8, n);
    errdefer allocator.free(casts);
    @memset(casts, null);
    var q = q_in;
    // A Compute whose evaluation was self-pushed into the scan's workers
    // (a terminal chain) still maps its slots the same way; the scan then
    // checks that the sink it carries is exactly that chain.
    var terminal_chain: ?*anyopaque = null;
    const stage: ?*Stage = while (true) {
        if (exec.queryAs(MatScan, q)) |ms| {
            if (ms.skip_col != null) break declined(reason, "MatScan with a slice-range skip");
            break ms.stage;
        }
        if (exec.queryAs(exec.ParallelScan, q)) |ps| {
            break ps.plainStageSource(map, terminal_chain) orelse declined(reason, "ParallelScan already started or carrying fused/pending work");
        }
        if (exec.queryAs(exec.AliasRename, q)) |ar| {
            if (ar.probe_fused and terminal_chain == null) break declined(reason, "AliasRename with a fused probe");
            q = ar.upstream;
            continue;
        }
        if (exec.queryAs(project_limit.Project, q)) |p| {
            if (p.probe_fused) break declined(reason, "Project with a fused probe");
            for (map) |*m| m.* = p.column_map[m.*];
            q = p.upstream;
            continue;
        }
        if (exec.queryAs(exec.Compute, q)) |c| {
            if (c.chain) |cf| {
                if (cf.inner != null) break declined(reason, "Compute chained under a join's probe");
                if (terminal_chain != null) break declined(reason, "stacked self-pushed Computes");
                terminal_chain = cf;
            }
            if (!peelComputeSlots(c, map, casts)) break declined(reason, "Compute deriving more than renames / in-place casts");
            q = c.upstream;
            continue;
        }
        break declined(reason, "operator is not a stage read, alias, projection or compute");
    };
    const st = stage orelse {
        allocator.free(map);
        allocator.free(casts);
        return null;
    };
    return .{ .stage = st, .map = map, .casts = casts };
}

fn declined(reason: ?*[]const u8, why: []const u8) ?*Stage {
    if (reason) |r| r.* = why;
    return null;
}

/// Rewrite `map`/`casts` (in `c`'s output space) into `c`'s upstream space.
/// False when a slot reads anything but a rename or an in-place `to_*` cast.
fn peelComputeSlots(c: *const exec.Compute, map: []usize, casts: []?[]const u8) bool {
    for (map, casts) |*m, *cast| {
        const k = derivedIndexAt(c, m.*) orelse continue;
        switch (c.derived[k].kind) {
            .rename => |rn| m.* = rn.src_idx,
            .call => {
                if (cast.* != null or c.udf_registry != null) return false;
                const call = switch (c.derived_ir[k].expr) {
                    .call => |cl| cl,
                    else => return false,
                };
                if (call.args.len != 1 or !std.ascii.startsWithIgnoreCase(call.fn_name, "to_")) return false;
                const arg = switch (call.args[0]) {
                    .col_ref => |name| name,
                    else => return false,
                };
                if (!types.columnNameEql(arg, c.derived_ir[k].name)) return false;
                if (m.* >= c.upstream.outputSchema().len) return false;
                cast.* = call.fn_name;
            },
            else => return false,
        }
    }
    return true;
}

fn derivedIndexAt(c: *const exec.Compute, out_idx: usize) ?usize {
    for (c.derived_output_indices, 0..) |oi, k| {
        if (oi == out_idx) return k;
    }
    return null;
}

/// Accumulates a stage's pull-copied result as ONE contiguous store per
/// column (adopted-style; per-column arenas so the sweep-free ownership
/// story matches window adoption), for stages a downstream window wants to
/// borrow from. Fixed-width buffers pre-reserve the stage's row bound.
pub const ContigSink = struct {
    allocator: Allocator,
    stores: []engine.ColumnStore,
    arenas: []std.heap.ArenaAllocator,
    arena_backed: []bool,
    rows: u64 = 0,
    taken: bool = false,

    pub fn init(allocator: Allocator, schema: []const Column, expect_rows: usize) !ContigSink {
        const stores = try allocator.alloc(engine.ColumnStore, schema.len);
        errdefer allocator.free(stores);
        const arenas = try allocator.alloc(std.heap.ArenaAllocator, schema.len);
        errdefer allocator.free(arenas);
        const arena_backed = try allocator.alloc(bool, schema.len);
        errdefer allocator.free(arena_backed);
        @memset(arena_backed, true);
        for (arenas) |*a| a.* = std.heap.ArenaAllocator.init(std.heap.c_allocator);
        errdefer for (arenas) |*a| a.deinit();
        for (schema, stores, arenas) |sc, *st, *ar| {
            st.* = try engine.ColumnStore.initCapacity(ar.allocator(), sc.type, sc.nullable, expect_rows, 0);
        }
        return .{ .allocator = allocator, .stores = stores, .arenas = arenas, .arena_backed = arena_backed };
    }

    pub fn append(self: *ContigSink, batch: exec.Batch) !void {
        for (self.stores, self.arenas, 0..) |*st, *ar, ci| {
            try engine.transform.appendAllColumn(ar.allocator(), batch.values[ci], st);
        }
        self.rows += batch.row_count;
    }

    /// Size every store once for a fill whose totals are known up front.
    pub fn reserve(self: *ContigSink, rows: usize, str_bytes: []const u64) !void {
        const total: usize = @as(usize, @intCast(self.rows)) + rows;
        for (self.stores, self.arenas, str_bytes) |*st, *ar, b| try st.reserveTotal(ar.allocator(), total, @intCast(b));
    }

    /// Append only the picked rows — the scan-once partition router's gather.
    pub fn appendIndices(self: *ContigSink, batch: exec.Batch, indices: []const u32) !void {
        for (self.stores, self.arenas, 0..) |*st, *ar, ci| {
            try engine.transform.appendByIndices(ar.allocator(), batch.values[ci], indices, st);
        }
        self.rows += indices.len;
    }

    pub fn take(self: *ContigSink) MaterializedResult.Adopted {
        self.taken = true;
        return .{ .stores = self.stores, .arenas = self.arenas, .arena_backed = self.arena_backed };
    }

    pub fn deinit(self: *ContigSink) void {
        if (self.taken) return;
        for (self.arenas) |*a| a.deinit();
        self.allocator.free(self.stores);
        self.allocator.free(self.arenas);
        self.allocator.free(self.arena_backed);
    }
};

/// One materialization boundary: a compiled pipeline, run at most once, and
/// its result's consumer accounting.
pub const Stage = struct {
    allocator: Allocator,
    /// The stage's compiled pipeline. Drained and torn down by `ensureRun`;
    /// torn down by StageSet if the stage never ran (error paths, LIMIT
    /// upstream cutting the plan short).
    query: exec.Query,
    query_alive: bool = true,
    /// Deep copy of the pipeline's output schema (StageSet arena) — the
    /// pipeline itself is gone after `ensureRun`, but MatScan batches and
    /// the result keep referring to this.
    schema: []const Column,
    /// The pipeline's compile-time output stats, captured at `addStage`
    /// before the pipeline can be torn down (column stats and sort keys are
    /// arena copies). These are provable bounds on the materialized result —
    /// the result preserves the pipeline's row order, so the sort state
    /// carries over verbatim. `ensureRun` tightens `upper_rows` to the exact
    /// materialized count and caps the NDV bounds, so consumers compiled (or
    /// re-querying stats) after the stage ran see exact figures.
    stats_upper_rows: u64,
    sort_state: exec.SortState,
    col_stats: []exec.ColStat,
    result: ?*MaterializedResult = null,
    /// Consumers bound at compile time / consumers finished. When the last
    /// finishes, the result frees in the background. Atomic: concurrent
    /// consumer pipelines may register (compile) and release (deinit) from
    /// their own threads; the FINAL release is still single-threaded.
    uses_total: std.atomic.Value(usize) = .init(0),
    uses_done: std.atomic.Value(usize) = .init(0),
    free_thread: ?std.Thread = null,
    /// Query-scoped accountant the buffered result is charged against
    /// (null = no tracking). All reserve/release calls happen on the
    /// driving connection thread — the background free thread only frees
    /// memory, never touches accounting.
    accountant: ?*exec.memory.MemoryAccountant = null,
    reserved_bytes: usize = 0,
    /// FastTable bytes reserved for shared join builds (released with the
    /// result, under the join-build category).
    join_build_reserved: usize = 0,
    join_build_lock: std.atomic.Mutex = .unlocked,
    /// Compile-order index in the StageSet — a stable label for `--profile-ops`
    /// per-CTE timing (`[cte]` lines), nothing more.
    id: usize = 0,
    /// The CTE name behind this stage (empty for synthetic wraps), shown
    /// next to the id on the `[cte]` lines.
    name: []const u8 = "",
    /// `--profile-ops` only: ticks spent compiling this block's body (setup),
    /// recorded by the staged compiler before the drain runs.
    setup_ticks: i64 = 0,
    /// Window-output-as-stage: when the compiled pipeline IS a Window
    /// operator (set via exec.queryAs by the staged compiler), ensureRun
    /// adopts its materialized buffers zero-copy instead of pull-copying
    /// the emit stream.
    adopt_window: ?*window_mod.Window = null,
    /// TVF-output-as-stage: when the compiled pipeline IS a TableFnExec
    /// (set via exec.queryAs by the staged compiler), ensureRun adopts its
    /// output ColumnStores zero-copy instead of pull-copying the emit —
    /// same contract as adopt_window.
    adopt_table_fn: ?*table_fn_mod.TableFnExec = null,
    /// Column borrowing: this stage's adopted result contains shallow
    /// references into an upstream stage's contiguous buffers (the window
    /// borrowed its pass-through input columns), so that stage must stay
    /// alive as long as this result. One use registered at compile,
    /// released when this stage's result frees. Pins chain transitively.
    pinned_upstream: ?*Stage = null,
    /// A compile-time borrower exists downstream (a window on a row-aligned
    /// chain): materialize this stage's result as contiguous columns
    /// (adopted-style, view chunks) instead of per-chunk stores, so the
    /// borrower can take shallow references. Set by the staged compiler
    /// BEFORE any run; a stage that already ran eagerly just misses the
    /// optimization (the borrower's bind degrades to normal accumulation).
    want_contiguous: bool = false,
    /// Threads for the contiguous fill when the pipeline's batch data is
    /// stable (`Query.stableData`) — a union of materialized stages fills
    /// its 700MB contiguous copy in parallel instead of one serial append
    /// loop. Set by the staged compiler alongside `want_contiguous`.
    fill_dop: usize = 1,

    pub fn ensureRun(self: *Stage) anyerror!void {
        if (self.result != null) return;
        if (!self.query_alive) {
            std.debug.print("[stage] ensureRun after teardown: stage#{d} (result={})\n", .{ self.id, self.result != null });
            return error.UnsupportedQueryShape; // re-run after teardown: can't happen via MatScan
        }
        const prof_on = exec.prof.enabled;
        const w0 = if (prof_on) exec.prof.nowTicks() else 0;
        const child0 = if (prof_on) exec.prof.cteChildTicks() else 0;
        const snap = if (prof_on) exec.prof.snapSlots() else undefined;
        var append_ticks: i64 = 0;
        const res = try self.allocator.create(MaterializedResult);
        errdefer self.allocator.destroy(res);
        res.* = .{ .allocator = self.allocator, .schema = self.schema, .expected_rows = self.stats_upper_rows };
        errdefer res.deinitChunks();
        errdefer self.releaseReserved();
        const row_bytes = exec.memory.estimateRowBytes(self.schema);
        if (self.adopt_window) |win| {
            try win.ensureDrained();
            if (self.accountant) |acct| {
                // Borrowed pass-through columns are shallow references into
                // the pinned upstream stage (charged there); bill only what
                // the adopted result owns or a window chain pays N times.
                const bytes = win.adoptedRowBytesEstimate() * @as(usize, @intCast(win.accumulated_rows));
                try acct.reserve(.materialize, bytes);
                self.reserved_bytes += bytes;
            }
            const ad = try win.adoptBuffers();
            try res.adoptContiguous(
                .{ .stores = ad.stores, .arenas = ad.arenas, .arena_backed = ad.arena_backed },
                ad.rows,
            );
        } else if (self.adopt_table_fn) |tf| {
            if (exec.prof.enabled) std.debug.print("[stage-tvfadopt] stage#{d}\n", .{self.id});
            try tf.ensureExecuted();
            if (self.accountant) |acct| {
                const bytes = row_bytes * @as(usize, @intCast(tf.output_cols[0].rowCount()));
                try acct.reserve(.materialize, bytes);
                self.reserved_bytes += bytes;
            }
            const ad = try tf.adoptBuffers();
            try res.adoptContiguous(
                .{ .stores = ad.stores, .arenas = ad.arenas, .arena_backed = ad.arena_backed },
                ad.rows,
            );
        } else if (self.want_contiguous) {
            var contig = try ContigSink.init(self.allocator, self.schema, self.expectedRowsHint());
            errdefer contig.deinit();
            if (self.fill_dop > 1 and self.query.stableData()) {
                try self.fillContigParallel(&contig, row_bytes, prof_on, &append_ticks);
            } else while (try self.query.next()) |batch| {
                if (self.accountant) |acct| {
                    const bytes = row_bytes * batch.row_count;
                    try acct.reserve(.materialize, bytes);
                    self.reserved_bytes += bytes;
                }
                const a0 = if (prof_on) exec.prof.nowTicks() else 0;
                try contig.append(batch);
                if (prof_on) append_ticks += exec.prof.nowTicks() - a0;
            }
            try res.adoptContiguous(contig.take(), contig.rows);
        } else if (try self.query.takeOwnedChunks()) |oc| {
            // The pipeline materialized in its scan workers and handed the
            // buffers over — adopt them; the pull-copy below never runs.
            if (self.accountant) |acct| {
                var rows: u64 = 0;
                for (oc.chunks) |c| rows += c.rows;
                const bytes = row_bytes * @as(usize, @intCast(rows));
                acct.reserve(.materialize, bytes) catch |e| {
                    exec.deinitOwnedChunks(oc);
                    return e;
                };
                self.reserved_bytes += bytes;
            }
            const a0 = if (prof_on) exec.prof.nowTicks() else 0;
            const n_adopted = oc.chunks.len;
            try res.adoptOwnedChunks(oc);
            if (prof_on) {
                append_ticks = -1; // suppress the copy line; adoption is not a copy
                std.debug.print("[stage-adopt] stage#{d} chunks={d} rows={d} ({d:.1}ms)\n", .{
                    self.id, n_adopted, res.total_rows, exec.prof.ticksToMs(exec.prof.nowTicks() - a0),
                });
            }
        } else while (try self.query.next()) |batch| {
            if (self.accountant) |acct| {
                const bytes = row_bytes * batch.row_count;
                try acct.reserve(.materialize, bytes);
                self.reserved_bytes += bytes;
            }
            const a0 = if (prof_on) exec.prof.nowTicks() else 0;
            try res.appendBatch(batch);
            if (prof_on) append_ticks += exec.prof.nowTicks() - a0;
        }
        // Release the pipeline's operator buffers right away — the stage's
        // working memory drops to just the chunked result.
        const td0 = if (prof_on) exec.prof.nowTicks() else 0;
        self.query.deinit();
        const teardown = if (prof_on) exec.prof.nowTicks() - td0 else 0;
        self.query_alive = false;
        self.result = res;
        self.stats_upper_rows = res.total_rows;
        exec.capColStats(self.col_stats, res.total_rows);
        if (prof_on) {
            const wall = exec.prof.nowTicks() - w0;
            // Nested upstream stages triggered lazily during this drain charge
            // their own wall to cte_child_ticks; subtract so this line is SELF.
            const children: i64 = @intCast(exec.prof.cteChildTicks() - child0);
            exec.prof.dumpStageDelta(self.id, self.name, res.total_rows, wall - children, wall, self.setup_ticks, teardown, snap);
            // Charge the parent this stage's SELF wall: the nested runs
            // below already charged theirs, so a full wall here would
            // count grandchildren twice at the parent.
            exec.prof.addCteChildTicks(@intCast(@max(wall - children, 0)));
            if (append_ticks > 0) std.debug.print("[stage-append] stage#{d} copy={d:.1}ms rows={d} contig={}\n", .{
                self.id, exec.prof.ticksToMs(append_ticks), res.total_rows, self.want_contiguous,
            });
        }
    }

    /// Parallel contiguous fill over a stable-data pipeline: collect every
    /// batch's view structs (the DATA is stable until the pipeline tears
    /// down — `Query.stableData`), size each (batch, column) destination
    /// once, then fill disjoint batch ranges on `fill_dop` threads with
    /// positional writes (no allocation). Worker cut points land only on
    /// 8-row-aligned destination offsets so no two workers share a validity
    /// byte. Falls back to the serial path when a string column would
    /// overflow positional (u32-offset) storage.
    fn fillContigParallel(self: *Stage, contig: *ContigSink, row_bytes: usize, prof_on: bool, append_ticks: *i64) !void {
        var scratch = std.heap.ArenaAllocator.init(self.allocator);
        defer scratch.deinit();
        const a = scratch.allocator();
        const ncols = self.schema.len;

        const Collected = struct { views: []ColumnView, rows: usize };
        var batches: std.ArrayListUnmanaged(Collected) = .empty;
        const str_bytes = try a.alloc(u64, ncols);
        @memset(str_bytes, 0);
        var total: usize = 0;
        while (try self.query.next()) |batch| {
            if (self.accountant) |acct| {
                const bytes = row_bytes * batch.row_count;
                try acct.reserve(.materialize, bytes);
                self.reserved_bytes += bytes;
            }
            if (batch.row_count == 0) continue;
            const vs = try a.alloc(ColumnView, batch.values.len);
            @memcpy(vs, batch.values);
            for (vs, 0..) |v, ci| switch (v.data) {
                .varchar, .string, .char, .json => |sv| str_bytes[ci] += sv.offsets[batch.row_count] - sv.offsets[0],
                else => {},
            };
            try batches.append(a, .{ .views = vs, .rows = batch.row_count });
            total += batch.row_count;
        }
        if (total == 0) return;

        const a0 = if (prof_on) exec.prof.nowTicks() else 0;
        // Positional string writes need u32 offsets end to end; a column
        // that would overflow falls back to the serial range appends.
        for (str_bytes) |b| {
            if (b > std.math.maxInt(u32)) {
                for (batches.items) |b2| {
                    try contig.append(.{ .schema = self.schema, .values = b2.views, .row_count = b2.rows });
                }
                if (prof_on) append_ticks.* += exec.prof.nowTicks() - a0;
                return;
            }
        }

        // Each (batch, column) prepare below would otherwise regrow the
        // store, and a source that emits many batches (a parallel group
        // emit) regrew it many times: size each once for the total.
        try contig.reserve(total, str_bytes);
        const preps = try a.alloc(engine.transform.PreparedAppend, batches.items.len * ncols);
        for (batches.items, 0..) |b, bi| {
            for (0..ncols) |ci| {
                preps[bi * ncols + ci] = try engine.transform.prepareAppend(
                    contig.arenas[ci].allocator(),
                    b.views[ci],
                    b.rows,
                    &contig.stores[ci],
                );
            }
        }

        const Fill = struct {
            batches: []const Collected,
            preps: []const engine.transform.PreparedAppend,
            stores: []engine.ColumnStore,
            ncols: usize,
            fn run(f: *const @This(), lo: usize, hi: usize) void {
                for (f.batches[lo..hi], lo..) |b, bi| {
                    for (0..f.ncols) |ci| {
                        engine.transform.writeAppendSlice(b.views[ci], 0, b.rows, &f.stores[ci], f.preps[bi * f.ncols + ci]);
                    }
                }
            }
        };
        const fill = Fill{ .batches = batches.items, .preps = preps, .stores = contig.stores, .ncols = ncols };

        // Cut worker ranges only where the destination offset is 8-aligned
        // (batch bases are cumulative row counts; preps[bi*ncols].base_row is
        // the batch's base for every column alike).
        const n_workers = @min(self.fill_dop, batches.items.len);
        const cuts = try a.alloc(usize, n_workers + 1);
        cuts[0] = 0;
        var w: usize = 1;
        var bi: usize = 1;
        while (w < n_workers) : (w += 1) {
            const target = w * total / n_workers;
            while (bi < batches.items.len and
                (preps[bi * ncols].base_row < target or preps[bi * ncols].base_row % 8 != 0)) : (bi += 1)
            {}
            cuts[w] = bi;
        }
        cuts[n_workers] = batches.items.len;

        const threads = try a.alloc(?std.Thread, n_workers);
        for (threads, 0..) |*t, i| {
            if (cuts[i + 1] <= cuts[i]) {
                t.* = null;
                continue;
            }
            t.* = std.Thread.spawn(.{}, Fill.run, .{ &fill, cuts[i], cuts[i + 1] }) catch blk: {
                fill.run(cuts[i], cuts[i + 1]);
                break :blk null;
            };
        }
        for (threads) |t| {
            if (t) |th| th.join();
        }
        contig.rows = total;
        if (prof_on) {
            append_ticks.* += exec.prof.nowTicks() - a0;
            std.debug.print("[stage-parfill] stage#{d} batches={d} rows={d} workers={d}\n", .{
                self.id, batches.items.len, total, n_workers,
            });
        }
    }

    fn expectedRowsHint(self: *const Stage) usize {
        const cap: u64 = 1 << 22; // don't pre-reserve absurd compile-time bounds
        return @intCast(@min(self.stats_upper_rows, cap));
    }

    fn releaseReserved(self: *Stage) void {
        if (self.accountant) |acct| {
            if (self.reserved_bytes > 0) acct.release(.materialize, self.reserved_bytes);
            if (self.join_build_reserved > 0) acct.release(.join_build, self.join_build_reserved);
        }
        self.reserved_bytes = 0;
        self.join_build_reserved = 0;
    }

    /// The hash-join build over this stage's result for `keys` (stage
    /// columns with their casts, in join-pair order), with every cast in
    /// `casts` evaluated once over its column; built on first request and
    /// shared afterwards. `needs_chain` distinguishes a FULL join's table.
    /// Runs the stage if it hasn't run. Null when the key types have no
    /// fast lane or a cast has no plain kernel (the join keeps its own
    /// general build).
    pub fn sharedJoinBuild(self: *Stage, keys: []const KeySpec, casts: []const ColumnCast, needs_chain: bool) !?SharedJoinBuild {
        try self.ensureRun();
        const res = self.result orelse return null;
        if (res.total_rows > std.math.maxInt(u32)) return null;
        while (!self.join_build_lock.tryLock()) std.atomic.spinLoopHint();
        defer self.join_build_lock.unlock();
        for (res.join_builds.items) |*jb| {
            if (jb.matches(keys, casts, needs_chain)) return jb.*;
        }
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        errdefer arena.deinit();
        const aa = arena.allocator();
        const views = try aa.alloc(ColumnView, res.schema.len);
        var copies: []engine.ColumnStore = &.{};
        try res.contiguousViews(views, &copies);
        errdefer {
            for (copies) |*c| c.deinit(self.allocator);
            if (copies.len > 0) self.allocator.free(copies);
        }
        const rows: u32 = @intCast(res.total_rows);
        var cast_stores: []engine.ColumnStore = &.{};
        if (casts.len > 0) cast_stores = try self.allocator.alloc(engine.ColumnStore, casts.len);
        var casts_done: usize = 0;
        errdefer {
            for (cast_stores[0..casts_done]) |*c| c.deinit(self.allocator);
            if (cast_stores.len > 0) self.allocator.free(cast_stores);
        }
        for (casts, 0..) |cst, i| {
            if (cst.col >= res.schema.len) return null;
            cast_stores[i] = (try castStageColumn(self.allocator, aa, cst.fn_name, views[cst.col], res.schema[cst.col].type, rows)) orelse return null;
            casts_done += 1;
        }
        var key_views: [join_mod.MAX_FAST_KEYS]ColumnView = undefined;
        for (keys, 0..) |k, i| {
            key_views[i] = if (k.cast) |fn_name| blk: {
                for (casts, cast_stores) |c, *st| {
                    if (c.col == k.col and std.ascii.eqlIgnoreCase(c.fn_name, fn_name)) break :blk st.view();
                }
                return null;
            } else views[k.col];
        }
        const bytes = join_mod.fastTableBytes(rows, needs_chain);
        if (self.accountant) |acct| try acct.reserve(.join_build, bytes);
        errdefer if (self.accountant) |acct| acct.release(.join_build, bytes);
        const built = (try join_mod.buildFastTable(aa, self.allocator, key_views[0..keys.len], rows, needs_chain, @max(self.fill_dop, join_mod.defaultBuildThreads()))) orelse return null;
        self.join_build_reserved += bytes;
        const owned_casts = try aa.alloc(ColumnCast, casts.len);
        for (casts, owned_casts) |c, *o| o.* = .{ .col = c.col, .fn_name = try aa.dupe(u8, c.fn_name) };
        const owned_keys = try aa.alloc(KeySpec, keys.len);
        for (keys, owned_keys) |k, *o| o.* = .{ .col = k.col, .cast = if (k.cast) |f| try aa.dupe(u8, f) else null };
        const jb: SharedJoinBuild = .{
            .keys = owned_keys,
            .casts = owned_casts,
            .needs_chain = needs_chain,
            .arena = arena,
            .copies = copies,
            .cast_stores = cast_stores,
            .views = views,
            .rows = rows,
            .table = built.table,
            .keys_unique = built.keys_unique,
        };
        try res.join_builds.append(self.allocator, jb);
        return jb;
    }

    /// Register a consumer at compile time — parity with `MatScan.create`'s
    /// `uses_total` bump. A parallel buffer scan (which fans out to N workers
    /// over the one immutable result) counts as ONE use; it registers here and
    /// releases once at its own deinit.
    pub fn registerUse(self: *Stage) void {
        _ = self.uses_total.fetchAdd(1, .monotonic);
    }

    /// A consumer finished (fully drained or torn down). On the last one,
    /// hand the chunks to a background free so the teardown overlaps the
    /// next stage; the thread is joined in `deinit`. The budget is handed
    /// back HERE (driving thread, before the async free) so accounting
    /// stays deterministic for the caller.
    pub fn releaseUse(self: *Stage) void {
        const done = self.uses_done.fetchAdd(1, .acq_rel) + 1;
        if (done < self.uses_total.load(.acquire)) return;
        const res = self.result orelse return;
        self.result = null;
        self.releaseReserved();
        if (self.pinned_upstream) |src| {
            self.pinned_upstream = null;
            src.releaseUse();
        }
        if (std.Thread.spawn(.{}, freeResultThread, .{res})) |th| {
            self.free_thread = th;
        } else |_| {
            freeResultThread(res);
        }
    }

    pub fn deinit(self: *Stage) void {
        if (self.free_thread) |th| th.join();
        if (self.result) |res| freeResultThread(res);
        self.releaseReserved();
        if (self.query_alive) self.query.deinit();
        self.allocator.destroy(self);
    }
};

fn freeResultThread(res: *MaterializedResult) void {
    const allocator = res.allocator;
    res.deinitChunks();
    allocator.destroy(res);
}

/// Owns every Stage of one staged query plus the arena backing their schema
/// copies. Torn down by the StagedRoot wrapper after the root pipeline.
pub const StageSet = struct {
    allocator: Allocator,
    arena: std.heap.ArenaAllocator,
    stages: std.ArrayListUnmanaged(*Stage) = .empty,

    pub fn create(allocator: Allocator) !*StageSet {
        const self = try allocator.create(StageSet);
        self.* = .{ .allocator = allocator, .arena = std.heap.ArenaAllocator.init(allocator) };
        return self;
    }

    /// Wrap a compiled stage pipeline. Takes ownership of `query` (even on
    /// error). The schema is deep-copied into the set's arena. `accountant`
    /// (null = no tracking) is charged for the buffered result while it
    /// lives — reserved as the stage drains, released when the last reader
    /// finishes.
    pub fn addStage(self: *StageSet, query: exec.Query, accountant: ?*exec.memory.MemoryAccountant) !*Stage {
        var q = query;
        errdefer q.deinit();
        const aa = self.arena.allocator();
        const src = q.outputSchema();
        const schema = try aa.alloc(Column, src.len);
        for (src, 0..) |c, i| {
            schema[i] = .{
                .name = try aa.dupe(u8, c.name),
                .type = c.type,
                .nullable = c.nullable,
            };
        }
        const src_stats = q.stats();
        // A per-column array of the wrong length carries no usable mapping —
        // treat it like "no information" rather than misattribute bounds.
        const col_stats = try aa.dupe(
            exec.ColStat,
            if (src_stats.column_stats.len == src.len) src_stats.column_stats else &.{},
        );
        const sort_keys = try aa.alloc([]const u8, src_stats.sort_state.keys.len);
        for (src_stats.sort_state.keys, sort_keys) |k, *d| d.* = try aa.dupe(u8, k);
        const sort_state: exec.SortState = .{
            .keys = sort_keys,
            .descs = try aa.dupe(bool, src_stats.sort_state.descs),
            .global = src_stats.sort_state.global,
        };
        const stage = try self.allocator.create(Stage);
        errdefer self.allocator.destroy(stage);
        stage.* = .{
            .allocator = self.allocator,
            .query = q,
            .schema = schema,
            .stats_upper_rows = src_stats.upper_rows,
            .sort_state = sort_state,
            .col_stats = col_stats,
            .accountant = accountant,
            .id = self.stages.items.len,
            // Compile pin: chains over stages run EAGERLY during compilation
            // (createOverStage's barrier), and such a run can fully drain an
            // upstream stage whose later consumers haven't compiled (and so
            // haven't registered) yet — without the pin, the result would
            // free out from under them. Released by releaseCompilePins once
            // the whole plan is built.
            .uses_total = .init(1),
        };
        try self.stages.append(self.allocator, stage);
        return stage;
    }

    /// Release every stage's compile pin (see addStage) — call exactly once,
    /// after the whole plan (all stage bodies + the root block) has compiled
    /// and every real consumer is registered. A stage fully drained during an
    /// eager compile-time run frees here; the rest free when their last
    /// runtime reader finishes.
    pub fn releaseCompilePins(self: *StageSet) void {
        for (self.stages.items) |stage| stage.releaseUse();
    }

    pub fn deinit(self: *StageSet) void {
        var i = self.stages.items.len;
        while (i > 0) {
            i -= 1;
            self.stages.items[i].deinit();
        }
        self.stages.deinit(self.allocator);
        self.arena.deinit();
        self.allocator.destroy(self);
    }
};

/// Scan leaf over a stage's materialized result: one chunk per `next()`,
/// views borrowed straight from the chunk's column stores (zero copy).
/// Triggers the stage's (and transitively its upstreams') execution on
/// first use; releases its use on exhaustion so the result can free while
/// downstream operators keep working.
pub const MatScan = struct {
    allocator: Allocator,
    stage: *Stage,
    views: []ColumnView,
    cursor: usize = 0,
    released: bool = false,
    /// Set by `stableData`: a consumer holds batches past exhaustion, so
    /// the early (exhaustion-time) release of the stage use is skipped —
    /// only deinit releases. See `stableData`.
    stability_promised: bool = false,
    /// Slice-range chunk skip hint (see setSliceSkip).
    skip_col: ?[]const u8 = null,
    skip_lo: ?types.Value = null,
    skip_hi: ?types.Value = null,
    skip_want_null: bool = false,

    pub fn create(allocator: Allocator, stage: *Stage) !exec.Query {
        const views = try allocator.alloc(ColumnView, stage.schema.len);
        errdefer allocator.free(views);
        const self = try allocator.create(MatScan);
        self.* = .{ .allocator = allocator, .stage = stage, .views = views };
        stage.registerUse();
        return exec.makeQuery(allocator, self);
    }

    /// Keyed-region exit graft (slice-range reader hint): this scan feeds a
    /// range filter `col > lo AND col <= hi` (null bound = open; `want_null`
    /// when the filter also passes NULL keys). Chunks of a slice-adopted
    /// result carry key ranges — disjoint chunks are skipped wholesale. Pure
    /// optimization: the filter above still evaluates, so a wrong hint can
    /// only cost rows it would have discarded anyway... never invent rows.
    pub fn setSliceSkip(self: *MatScan, col: []const u8, lo: ?types.Value, hi: ?types.Value, want_null: bool) void {
        self.skip_col = col;
        self.skip_lo = lo;
        self.skip_hi = hi;
        self.skip_want_null = want_null;
    }

    fn chunkDisjoint(self: *const MatScan, chunk: *const MaterializedResult.Chunk) bool {
        if (self.skip_want_null and chunk.key_nulls) return false;
        if (self.skip_lo) |lo| {
            // Chunk holds values <= key_hi; filter needs values > lo.
            if (chunk.key_hi) |chi| {
                if (chi.compare(lo) != .gt) return true;
            }
        }
        if (self.skip_hi) |hi| {
            // Chunk holds values > key_lo; filter needs values <= hi.
            if (chunk.key_lo) |clo| {
                if (clo.compare(hi) != .lt) return true;
            }
        }
        return false;
    }

    pub fn next(self: *MatScan) !?exec.Batch {
        try self.stage.ensureRun();
        const res = self.stage.result orelse {
            self.releaseEarly();
            return null;
        };
        const skip_active = self.skip_col != null and res.sliced_key != null and
            types.columnNameEql(self.skip_col.?, res.sliced_key.?);
        while (self.cursor < res.chunks.items.len) {
            const chunk = res.chunks.items[self.cursor];
            self.cursor += 1;
            if (chunk.rows == 0) continue;
            if (skip_active and self.chunkDisjoint(&chunk)) continue;
            if (chunk.views.len > 0) {
                return exec.Batch{
                    .schema = self.stage.schema,
                    .values = chunk.views,
                    .row_count = chunk.rows,
                };
            }
            for (chunk.cols, 0..) |*col, i| self.views[i] = col.view();
            return exec.Batch{
                .schema = self.stage.schema,
                .values = self.views,
                .row_count = chunk.rows,
            };
        }
        self.releaseEarly();
        return null;
    }

    /// Exhaustion-time release — lets the buffer free as soon as the last
    /// streaming consumer finishes. Suppressed under a stability promise
    /// (the consumer still reads the data after the drain).
    fn releaseEarly(self: *MatScan) void {
        if (self.stability_promised) return;
        self.releaseOnce();
    }

    fn releaseOnce(self: *MatScan) void {
        if (self.released) return;
        self.released = true;
        self.stage.releaseUse();
    }

    pub fn deinit(self: *MatScan) void {
        self.releaseOnce();
        self.allocator.free(self.views);
        self.allocator.destroy(self);
    }

    pub fn outputSchema(self: *MatScan) []const Column {
        return self.stage.schema;
    }

    pub fn addPrune(_: *MatScan, _: exec.Predicate) !void {}

    /// Batches are views over the stage's immutable `MaterializedResult`.
    /// Answering true is a PROMISE: a stability-relying consumer (parallel
    /// reduce, parallel contiguous fill) holds batches past exhaustion, so
    /// the exhaustion-time early release of the stage use — which could
    /// free the result while the last consumer standing still reads it —
    /// is deferred to deinit.
    pub fn stableData(self: *MatScan) bool {
        self.stability_promised = true;
        return true;
    }

    pub fn stats(self: *MatScan) exec.PipelineStats {
        return .{
            .upper_rows = self.stage.stats_upper_rows,
            .sort_state = self.stage.sort_state,
            .column_stats = self.stage.col_stats,
        };
    }

    pub fn accountant(_: *MatScan) ?*exec.memory.MemoryAccountant {
        return null;
    }

    pub fn explain(self: *MatScan, out: *std.ArrayList(u8), allocator: Allocator, depth: usize) !void {
        var buf: [64]u8 = undefined;
        const line = std.fmt.bufPrint(&buf, "MatScan cols={d}", .{self.stage.schema.len}) catch "MatScan";
        try exec.explainLine(out, allocator, depth, line);
    }
};

/// A ranged, lock-free reader over a `[chunk_lo, chunk_hi)` stripe of a stage's
/// already-materialized result — the buffer-side analogue of a range-restricted
/// `Scan` worker. The parallel buffer scan hands each worker a disjoint stripe;
/// they read concurrently with no locks because the `MaterializedResult` is
/// immutable once `ensureRun` completes and every emitted `ColumnView` borrows
/// `[]const` chunk storage.
///
/// Unlike `MatScan`, `next` does NOT call `ensureRun` — the orchestrator runs
/// the stage ONCE (its barrier) before spawning workers, so a per-worker
/// `ensureRun` would race. `stats` reports the whole stage's stats (not just the
/// stripe), mirroring how a ranged table `Scan` reports whole-table stats — so
/// `workers[0].stats()` yields the full-result figures the routing layer needs.
///
/// Ownership: a ChunkRangeScan borrows the result; it never registers a stage
/// `use` (the single parent parallel-scan counts as one use and releases it).
pub const ChunkRangeScan = struct {
    allocator: Allocator,
    stage: *Stage,
    result: *const MaterializedResult,
    views: []ColumnView,
    range: Range,
    cursor: usize,

    /// Chunks [chunk_lo, chunk_hi) of the result, entered at `row_lo` of
    /// the first and left at `row_hi` of the last — a row stripe that may
    /// start and end mid-chunk, so a scan's units aren't bound to the
    /// stage's chunk count. `chunk_lo == chunk_hi` is empty. Mid-chunk
    /// bounds must be multiples of 8 (validity bitmaps slice by byte).
    pub const Range = struct {
        chunk_lo: usize,
        chunk_hi: usize,
        row_lo: usize,
        row_hi: usize,

        pub fn whole(result: *const MaterializedResult, lo: usize, hi: usize) Range {
            return .{
                .chunk_lo = lo,
                .chunk_hi = hi,
                .row_lo = 0,
                .row_hi = if (hi > lo) result.chunks.items[hi - 1].rows else 0,
            };
        }
    };

    /// Raw constructor returning the concrete pointer — the parallel buffer
    /// scan stores these in its worker array (parity with `Scan`'s raw alloc).
    pub fn alloc(allocator: Allocator, stage: *Stage, result: *const MaterializedResult, range: Range) !*ChunkRangeScan {
        const views = try allocator.alloc(ColumnView, stage.schema.len);
        errdefer allocator.free(views);
        const self = try allocator.create(ChunkRangeScan);
        self.* = .{
            .allocator = allocator,
            .stage = stage,
            .result = result,
            .views = views,
            .range = range,
            .cursor = range.chunk_lo,
        };
        return self;
    }

    pub fn create(allocator: Allocator, stage: *Stage, result: *const MaterializedResult, range: Range) !exec.Query {
        return exec.makeQuery(allocator, try alloc(allocator, stage, result, range));
    }

    pub fn next(self: *ChunkRangeScan) !?exec.Batch {
        while (self.cursor < self.range.chunk_hi) {
            const ci = self.cursor;
            self.cursor += 1;
            const chunk = self.result.chunks.items[ci];
            const lo = if (ci == self.range.chunk_lo) self.range.row_lo else 0;
            const hi = if (ci + 1 == self.range.chunk_hi) self.range.row_hi else chunk.rows;
            if (hi <= lo) continue;
            if (lo == 0 and hi == chunk.rows) {
                if (chunk.views.len > 0) {
                    return exec.Batch{
                        .schema = self.stage.schema,
                        .values = chunk.views,
                        .row_count = chunk.rows,
                    };
                }
                for (chunk.cols, 0..) |*col, i| self.views[i] = col.view();
                return exec.Batch{
                    .schema = self.stage.schema,
                    .values = self.views,
                    .row_count = chunk.rows,
                };
            }
            for (self.views, 0..) |*v, i| {
                const full = if (chunk.views.len > 0) chunk.views[i] else chunk.cols[i].view();
                v.* = engine.transform.subViewAligned(full, lo, hi - lo);
            }
            return exec.Batch{
                .schema = self.stage.schema,
                .values = self.views,
                .row_count = hi - lo,
            };
        }
        return null;
    }

    pub fn outputSchema(self: *ChunkRangeScan) []const Column {
        return self.stage.schema;
    }

    pub fn stats(self: *ChunkRangeScan) exec.PipelineStats {
        return .{
            .upper_rows = self.stage.stats_upper_rows,
            .sort_state = self.stage.sort_state,
            .column_stats = self.stage.col_stats,
        };
    }

    pub fn addPrune(_: *ChunkRangeScan, _: exec.Predicate) !void {}

    pub fn accountant(_: *ChunkRangeScan) ?*exec.memory.MemoryAccountant {
        return null;
    }

    pub fn deinit(self: *ChunkRangeScan) void {
        self.allocator.free(self.views);
        self.allocator.destroy(self);
    }

    pub fn explain(self: *ChunkRangeScan, out: *std.ArrayList(u8), allocator: Allocator, depth: usize) !void {
        var buf: [120]u8 = undefined;
        const r = self.range;
        const line = std.fmt.bufPrint(&buf, "ChunkRangeScan cols={d} chunks=[{d},{d}) row_lo={d} row_hi={d}", .{ self.stage.schema.len, r.chunk_lo, r.chunk_hi, r.row_lo, r.row_hi }) catch "ChunkRangeScan";
        try exec.explainLine(out, allocator, depth, line);
    }
};

test "stage captures pipeline stats and tightens them after the run" {
    const allocator = std.testing.allocator;

    const Stub = struct {
        allocator: Allocator,
        schema: [1]Column = .{.{ .name = "k", .type = .{ .bigint = {} }, .nullable = false }},
        col_stats: [1]exec.ColStat = .{.{ .ndv = .{ .exact = 7 }, .min = 1, .max = 9 }},

        pub fn next(_: *@This()) !?exec.Batch {
            return null;
        }
        pub fn deinit(self: *@This()) void {
            self.allocator.destroy(self);
        }
        pub fn outputSchema(self: *@This()) []const Column {
            return &self.schema;
        }
        pub fn addPrune(_: *@This(), _: exec.Predicate) !void {}
        pub fn stats(self: *@This()) exec.PipelineStats {
            return .{
                .upper_rows = 42,
                .sort_state = .{ .keys = &.{"k"}, .global = true },
                .column_stats = &self.col_stats,
            };
        }
        pub fn accountant(_: *@This()) ?*exec.memory.MemoryAccountant {
            return null;
        }
        pub fn explain(_: *@This(), out: *std.ArrayList(u8), a: Allocator, depth: usize) !void {
            try exec.explainLine(out, a, depth, "Stub");
        }
    };

    const stub = try allocator.create(Stub);
    stub.* = .{ .allocator = allocator };
    const sq = exec.makeQuery(allocator, stub);

    const set = try StageSet.create(allocator);
    defer set.deinit();
    const stage = try set.addStage(sq, null);

    var scan = try MatScan.create(allocator, stage);
    defer scan.deinit();

    // Pre-run: the boundary serves the source pipeline's provable bounds.
    const pre = scan.stats();
    try std.testing.expectEqual(@as(u64, 42), pre.upper_rows);
    try std.testing.expectEqual(@as(u32, 7), pre.column_stats[0].ndv.exact);
    try std.testing.expectEqual(@as(?i128, 1), pre.column_stats[0].min);
    try std.testing.expectEqualStrings("k", pre.sort_state.keys[0]);
    try std.testing.expect(pre.sort_state.global);

    // Drain (the stub emits nothing): exact figures replace the bounds.
    try std.testing.expect((try scan.next()) == null);
    const post = scan.stats();
    try std.testing.expectEqual(@as(u64, 0), post.upper_rows);
    try std.testing.expectEqual(@as(u32, 0), post.column_stats[0].ndv.exact);
    try std.testing.expectEqualStrings("k", post.sort_state.keys[0]);
}

test "ChunkRangeScan: disjoint stripes cover every row exactly once" {
    const allocator = std.testing.allocator;

    // A stub that emits all `n` rows of a single bigint column in one batch,
    // then null — the stage chunks them at `chunk_rows`.
    const EmitStub = struct {
        allocator: Allocator,
        schema: [1]Column = .{.{ .name = "v", .type = .{ .bigint = {} }, .nullable = false }},
        data: []i64,
        views: [1]ColumnView = undefined,
        emitted: bool = false,

        pub fn next(self: *@This()) !?exec.Batch {
            if (self.emitted) return null;
            self.emitted = true;
            self.views[0] = .{ .data = .{ .bigint = self.data }, .nulls = null };
            return exec.Batch{ .schema = &self.schema, .values = &self.views, .row_count = self.data.len };
        }
        pub fn deinit(self: *@This()) void {
            self.allocator.destroy(self);
        }
        pub fn outputSchema(self: *@This()) []const Column {
            return &self.schema;
        }
        pub fn addPrune(_: *@This(), _: exec.Predicate) !void {}
        pub fn stats(_: *@This()) exec.PipelineStats {
            return .{ .upper_rows = 0, .sort_state = .{}, .column_stats = &.{} };
        }
        pub fn accountant(_: *@This()) ?*exec.memory.MemoryAccountant {
            return null;
        }
        pub fn explain(_: *@This(), out: *std.ArrayList(u8), a: Allocator, depth: usize) !void {
            try exec.explainLine(out, a, depth, "EmitStub");
        }
    };

    // One chunk (3 rows) and a multi-chunk result (> chunk_rows) exercise both
    // the in-range and the stripe-boundary paths.
    const row_counts = [_]usize{ 3, chunk_rows + 100 };
    for (row_counts) |n| {
        const data = try allocator.alloc(i64, n);
        defer allocator.free(data);
        for (data, 0..) |*d, i| d.* = @intCast(i);

        const stub = try allocator.create(EmitStub);
        stub.* = .{ .allocator = allocator, .data = data };
        const sq = exec.makeQuery(allocator, stub);

        const set = try StageSet.create(allocator);
        defer set.deinit();
        const stage = try set.addStage(sq, null);
        try stage.ensureRun();
        const res = stage.result.?;
        try std.testing.expectEqual(@as(u64, n), res.total_rows);

        // Split the result two ways — whole-chunk stripes, then row stripes
        // cut inside the first chunk — and confirm each pair reads every row
        // exactly once, in order.
        const n_chunks = res.chunks.items.len;
        const mid = n_chunks / 2;
        const last_rows = res.chunks.items[n_chunks - 1].rows;
        const cut: usize = if (res.chunks.items[0].rows >= 64) 64 else 0;
        const stripe_sets = [_][2]ChunkRangeScan.Range{
            .{ ChunkRangeScan.Range.whole(res, 0, mid), ChunkRangeScan.Range.whole(res, mid, n_chunks) },
            .{
                .{ .chunk_lo = 0, .chunk_hi = 1, .row_lo = 0, .row_hi = cut },
                .{ .chunk_lo = 0, .chunk_hi = n_chunks, .row_lo = cut, .row_hi = last_rows },
            },
        };
        for (stripe_sets) |ranges| {
            var seen: u64 = 0;
            var expect_val: i64 = 0;
            for (ranges) |r| {
                var leaf = try ChunkRangeScan.create(allocator, stage, res, r);
                defer leaf.deinit();
                while (try leaf.next()) |batch| {
                    const vals = batch.values[0].data.bigint;
                    for (vals) |v| {
                        try std.testing.expectEqual(expect_val, v);
                        expect_val += 1;
                        seen += 1;
                    }
                }
            }
            try std.testing.expectEqual(@as(u64, n), seen);
        }
    }
}

/// Root wrapper: forwards everything to the outermost pipeline and tears the
/// StageSet down after it.
pub const StagedRoot = struct {
    allocator: Allocator,
    inner: exec.Query,
    set: *StageSet,

    pub fn create(allocator: Allocator, inner: exec.Query, set: *StageSet) !exec.Query {
        const self = try allocator.create(StagedRoot);
        self.* = .{ .allocator = allocator, .inner = inner, .set = set };
        return exec.makeQuery(allocator, self);
    }

    pub fn next(self: *StagedRoot) !?exec.Batch {
        return self.inner.next();
    }

    pub fn deinit(self: *StagedRoot) void {
        self.inner.deinit();
        self.set.deinit();
        self.allocator.destroy(self);
    }

    pub fn outputSchema(self: *StagedRoot) []const Column {
        return self.inner.outputSchema();
    }

    pub fn addPrune(self: *StagedRoot, pred: exec.Predicate) !void {
        return self.inner.addPrune(pred);
    }

    pub fn stats(self: *StagedRoot) exec.PipelineStats {
        return self.inner.stats();
    }

    pub fn accountant(self: *StagedRoot) ?*exec.memory.MemoryAccountant {
        return self.inner.accountant();
    }

    pub fn explain(self: *StagedRoot, out: *std.ArrayList(u8), allocator: Allocator, depth: usize) !void {
        try exec.explainLine(out, allocator, depth, "StagedRoot");
        try self.inner.explain(out, allocator, depth + 1);
    }
};
