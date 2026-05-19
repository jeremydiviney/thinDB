//! Session-scoped temp table namespace. Each connection's SessionState
//! lazily allocates one of these on the first `CREATE TEMP TABLE` and
//! owns its lifetime; on disconnect / RESET CONNECTION / DISCARD ALL
//! the namespace is destroyed and the per-session dir is removed.
//!
//! Storage layout: temp tables live under `<catalog_root>/_temp/<backend_id>/`,
//! one subdir per table. Each temp table is a normal Table — same
//! schema.bin / manifest.bin / segments/ layout, same memtable +
//! auto-flush + segment-writer path as a persistent table. The only
//! things that make it "temp" are the location and the session-scoped
//! lifetime: at namespace close the entire per-session dir is rmtree'd.
//!
//! Design: temp tables are intentionally NOT registered in the catalog
//! (Catalog stays unaware of per-session state). Resolution against a
//! session's temp namespace happens at the wire-layer compile step
//! BEFORE the catalog lookup runs.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const types = @import("../types.zig");
const TableSchema = types.TableSchema;

const api = @import("api.zig");
const Config = api.Config;
const Error = api.Error;
const TableOptions = api.TableOptions;
const Table = api.Table;

pub const temp_root_dir_name: []const u8 = "_temp";

pub const TempNamespace = struct {
    allocator: Allocator,
    io: Io,
    /// Open handle to the catalog's `_temp` parent — used to create and
    /// later `deleteTree` the per-session dir by its leaf name. Owned;
    /// closed in `close()`.
    parent_dir: Io.Dir,
    /// Owned: leaf name of the per-session dir inside `_temp/`.
    leaf_name: []u8,
    /// Open handle to the per-session dir. Closed in `close()` before
    /// the deleteTree fires.
    tmp_dir: Io.Dir,
    config: Config,
    tables: std.StringHashMapUnmanaged(*Table) = .empty,
    tables_mutex: Io.Mutex = .init,

    pub fn open(
        allocator: Allocator,
        io: Io,
        root_dir: Io.Dir,
        backend_id: u32,
        config: Config,
    ) !*TempNamespace {
        var parent_dir = try root_dir.createDirPathOpen(io, temp_root_dir_name, .{});
        errdefer parent_dir.close(io);

        const leaf_name = try std.fmt.allocPrint(allocator, "{d}", .{backend_id});
        errdefer allocator.free(leaf_name);

        const tmp_dir = try parent_dir.createDirPathOpen(io, leaf_name, .{});
        errdefer {
            var d = tmp_dir;
            d.close(io);
            parent_dir.deleteTree(io, leaf_name) catch {};
        }

        const self = try allocator.create(TempNamespace);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .io = io,
            .parent_dir = parent_dir,
            .leaf_name = leaf_name,
            .tmp_dir = tmp_dir,
            .config = config,
        };
        return self;
    }

    pub fn close(self: *TempNamespace) void {
        var it = self.tables.iterator();
        while (it.next()) |entry| {
            const t = entry.value_ptr.*;
            t.close();
            self.allocator.free(entry.key_ptr.*);
        }
        self.tables.deinit(self.allocator);
        self.tmp_dir.close(self.io);
        self.parent_dir.deleteTree(self.io, self.leaf_name) catch {};
        self.parent_dir.close(self.io);
        self.allocator.free(self.leaf_name);
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    /// True when a temp table with this name already exists in this session.
    pub fn contains(self: *TempNamespace, name: []const u8) bool {
        self.tables_mutex.lockUncancelable(self.io);
        defer self.tables_mutex.unlock(self.io);
        return self.tables.get(name) != null;
    }

    pub fn findTable(self: *TempNamespace, name: []const u8) ?*Table {
        self.tables_mutex.lockUncancelable(self.io);
        defer self.tables_mutex.unlock(self.io);
        return self.tables.get(name);
    }

    pub fn createTable(
        self: *TempNamespace,
        name: []const u8,
        table_schema: TableSchema,
        options: TableOptions,
    ) !*Table {
        try table_schema.validate();

        {
            self.tables_mutex.lockUncancelable(self.io);
            defer self.tables_mutex.unlock(self.io);
            if (self.tables.get(name) != null) return Error.TableAlreadyExists;
        }

        // Temp tables don't need WAL or fsync-on-flush: the whole
        // per-session dir is `deleteTree`'d at namespace close, and
        // any crash-leftover gets swept on the next Catalog.open.
        // Inheriting `wal_enabled=true` would fsync every insert for
        // no recovery benefit.
        var temp_config = self.config;
        temp_config.wal_enabled = false;
        temp_config.sync_mode = .none;

        const t = try Table.open(
            self.allocator,
            self.io,
            self.tmp_dir,
            name,
            table_schema,
            temp_config,
            options.row_group_size orelse temp_config.row_group_size,
        );
        errdefer t.close();

        const key_copy = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(key_copy);

        self.tables_mutex.lockUncancelable(self.io);
        defer self.tables_mutex.unlock(self.io);
        try self.tables.put(self.allocator, key_copy, t);
        return t;
    }

    pub fn dropTable(self: *TempNamespace, name: []const u8) !void {
        self.tables_mutex.lockUncancelable(self.io);
        const entry = self.tables.fetchRemove(name);
        self.tables_mutex.unlock(self.io);

        if (entry) |e| {
            const t = e.value;
            t.ddl_lock.lockUncancelable(t.io);
            t.close();
            self.allocator.free(e.key);
            self.tmp_dir.deleteTree(self.io, name) catch {};
            return;
        }
        return Error.TableNotFound;
    }

    /// Snapshot of every table name in this namespace. Caller frees each
    /// entry and the outer slice with `allocator`. Same shape as
    /// `Schema.listTables` so wire-layer callers can union the two.
    pub fn listTables(self: *TempNamespace, allocator: Allocator) ![][]u8 {
        self.tables_mutex.lockUncancelable(self.io);
        defer self.tables_mutex.unlock(self.io);

        const out = try allocator.alloc([]u8, self.tables.count());
        errdefer allocator.free(out);
        var i: usize = 0;
        errdefer for (out[0..i]) |s| allocator.free(s);
        var it = self.tables.keyIterator();
        while (it.next()) |k| : (i += 1) {
            out[i] = try allocator.dupe(u8, k.*);
        }
        return out;
    }
};

/// Best-effort sweep of stale `_temp/<id>/` dirs left behind by an
/// ungraceful crash. Called from `Catalog.open` / `Database.open` startup.
/// Failures are silently swallowed — startup must succeed even if the
/// sweep can't read the directory.
pub fn sweepStaleTempDirs(io: Io, root_dir: Io.Dir) void {
    root_dir.deleteTree(io, temp_root_dir_name) catch {};
}
