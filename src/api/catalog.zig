//! Catalog — top of the Catalog → Database → Schema → Table hierarchy.
//! Owns a root directory containing zero or more named Databases. Created
//! either explicitly via `Catalog.open` or implicitly by the back-compat
//! `Database.open` constructor.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const api = @import("api.zig");
const Config = api.Config;
const Error = api.Error;

const DatabaseMod = @import("database.zig");
const Database = DatabaseMod.Database;

const snapshot = @import("../util/snapshot.zig");

pub const Catalog = struct {
    allocator: Allocator,
    io: Io,
    root_dir: Io.Dir,
    config: Config,
    databases: std.StringHashMap(*Database),
    /// True when the Catalog allocator-owns its struct (the user called
    /// `Catalog.open` and gets a `*Catalog`). False when the Catalog is
    /// nested inside another owner (currently unused; reserved).
    heap_owned: bool = true,

    databases_mutex: Io.Mutex = .init,

    pub fn open(
        allocator: Allocator,
        io: Io,
        root_dir: Io.Dir,
        config: Config,
    ) !*Catalog {
        return Catalog.openInPlace(allocator, io, root_dir, config);
    }

    pub fn openInPlace(
        allocator: Allocator,
        io: Io,
        root_dir: Io.Dir,
        config: Config,
    ) !*Catalog {
        const self = try allocator.create(Catalog);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .io = io,
            .root_dir = root_dir,
            .config = config,
            .databases = .init(allocator),
        };
        return self;
    }

    pub fn close(self: *Catalog) void {
        var it = self.databases.iterator();
        while (it.next()) |entry| entry.value_ptr.*.closeInPlace();
        self.databases.deinit();
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    pub fn database(self: *Catalog, name: []const u8) ?*Database {
        self.databases_mutex.lockUncancelable(self.io);
        defer self.databases_mutex.unlock(self.io);
        return self.databases.get(name);
    }

    pub fn createDatabase(self: *Catalog, name: []const u8) !*Database {
        self.databases_mutex.lockUncancelable(self.io);
        defer self.databases_mutex.unlock(self.io);

        if (self.databases.get(name) != null) return Error.DatabaseAlreadyExists;
        if (self.root_dir.openDir(self.io, name, .{})) |probe_| {
            var probe = probe_;
            probe.close(self.io);
            return Error.DatabaseAlreadyExists;
        } else |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        }

        const db = try Database.create(self.allocator, self.io, self.root_dir, name, self.config, self);
        errdefer db.closeInPlace();
        try self.databases.put(db.name, db);
        return db;
    }

    /// Get-or-create. Used by the back-compat `Database.open` shim to
    /// adopt an existing on-disk database directory without erroring.
    pub fn createOrOpenDatabase(self: *Catalog, name: []const u8) !*Database {
        self.databases_mutex.lockUncancelable(self.io);
        defer self.databases_mutex.unlock(self.io);

        if (self.databases.get(name)) |existing| return existing;
        const db = try Database.create(self.allocator, self.io, self.root_dir, name, self.config, self);
        errdefer db.closeInPlace();
        try self.databases.put(db.name, db);
        return db;
    }

    pub fn dropDatabase(self: *Catalog, name: []const u8) !void {
        self.databases_mutex.lockUncancelable(self.io);
        const maybe = self.databases.fetchRemove(name);
        self.databases_mutex.unlock(self.io);

        if (maybe) |entry| {
            entry.value.closeInPlace();
        } else {
            var probe = self.root_dir.openDir(self.io, name, .{}) catch |err| switch (err) {
                error.FileNotFound => return Error.DatabaseNotFound,
                else => return err,
            };
            probe.close(self.io);
        }

        try self.root_dir.deleteTree(self.io, name);
    }

    pub fn listDatabases(self: *Catalog, allocator: Allocator) ![][]u8 {
        self.databases_mutex.lockUncancelable(self.io);
        defer self.databases_mutex.unlock(self.io);

        const out = try allocator.alloc([]u8, self.databases.count());
        errdefer allocator.free(out);
        var i: usize = 0;
        errdefer for (out[0..i]) |s| allocator.free(s);
        var it = self.databases.keyIterator();
        while (it.next()) |k| : (i += 1) {
            out[i] = try allocator.dupe(u8, k.*);
        }
        return out;
    }

    /// Walk every database (and every schema beneath each) and run one
    /// flush sweep. Errors from individual sweeps are swallowed — the
    /// background loop keeps running.
    pub fn backgroundFlushSweep(self: *Catalog) !void {
        const names = try snapshot.snapshotMapKeys(self.allocator, self.io, &self.databases_mutex, self.databases);
        defer snapshot.freeNames(self.allocator, names);
        for (names) |name| {
            const db = self.database(name) orelse continue;
            db.backgroundFlushSweep() catch {};
        }
    }

    pub fn runBackgroundFlusher(
        self: *Catalog,
        sleeper_io: Io,
        poll_ms: u32,
        should_stop: *std.atomic.Value(bool),
    ) void {
        const duration: Io.Duration = .fromMilliseconds(@intCast(poll_ms));
        while (!should_stop.load(.acquire)) {
            Io.sleep(sleeper_io, duration, .awake) catch return;
            if (should_stop.load(.acquire)) return;
            self.backgroundFlushSweep() catch {};
        }
    }

    pub fn backgroundCompactSweep(self: *Catalog) !void {
        const names = try snapshot.snapshotMapKeys(self.allocator, self.io, &self.databases_mutex, self.databases);
        defer snapshot.freeNames(self.allocator, names);
        for (names) |name| {
            const db = self.database(name) orelse continue;
            db.backgroundCompactSweep() catch {};
        }
    }

    pub fn runBackgroundCompactor(
        self: *Catalog,
        sleeper_io: Io,
        poll_ms: u32,
        should_stop: *std.atomic.Value(bool),
    ) void {
        const duration: Io.Duration = .fromMilliseconds(@intCast(poll_ms));
        while (!should_stop.load(.acquire)) {
            Io.sleep(sleeper_io, duration, .awake) catch return;
            if (should_stop.load(.acquire)) return;
            self.backgroundCompactSweep() catch {};
        }
    }

};
