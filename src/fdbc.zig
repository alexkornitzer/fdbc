const std = @import("std");

const erl = @cImport({
    @cInclude("erl_nif.h");
    @cInclude("erl_driver.h");
});

const FDB_API_VERSION = 740;

const fdb = @cImport({
    @cDefine("FDB_API_VERSION", "730");
    @cInclude("foundationdb/fdb_c.h");
});

fn handleError(env: ?*erl.ErlNifEnv, code: c_int) erl.ERL_NIF_TERM {
    const reason = fdb.fdb_get_error(code);
    const len = std.mem.len(reason);
    var term: erl.ERL_NIF_TERM = undefined;
    const ptr = erl.enif_make_new_binary(env, len, &term);
    @memcpy(ptr[0..len], reason[0..len]);
    return erl.enif_make_tuple3(env, Atoms.ERROR, erl.enif_make_int(env, code), term);
}

fn handleResult(env: ?*erl.ErlNifEnv, code: c_int) erl.ERL_NIF_TERM {
    if (code == 0) {
        return Atoms.OK;
    }
    return handleError(env, code);
}

fn selectApiVersion(env: ?*erl.ErlNifEnv, argc: c_int, argv: [*c]const erl.ERL_NIF_TERM) callconv(.c) erl.ERL_NIF_TERM {
    var runtime_version: c_int = undefined;
    if (argc != 1) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_int(env, argv[0], &runtime_version) == 0) {
        return erl.enif_make_badarg(env);
    }
    return handleResult(env, fdb.fdb_select_api_version_impl(runtime_version, FDB_API_VERSION));
}

fn getMaxApiVersion(env: ?*erl.ErlNifEnv, argc: c_int, _: [*c]const erl.ERL_NIF_TERM) callconv(.c) erl.ERL_NIF_TERM {
    if (argc != 0) {
        return erl.enif_make_badarg(env);
    }
    const max_api_version: c_int = fdb.fdb_get_max_api_version();
    return erl.enif_make_int(env, max_api_version);
}

fn networkSetOption(env: ?*erl.ErlNifEnv, argc: c_int, argv: [*c]const erl.ERL_NIF_TERM) callconv(.c) erl.ERL_NIF_TERM {
    var option: fdb.FDBNetworkOption = undefined;
    var value: ?*u8 = undefined;
    var value_length: c_int = undefined;
    if (argc < 1 or argc > 2) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_uint(env, argv[0], &option) == 0) {
        return erl.enif_make_badarg(env);
    }
    if (argc == 2) {
        var binary: erl.ErlNifBinary = undefined;
        if (erl.enif_inspect_binary(env, argv[1], &binary) == 0) {
            return erl.enif_make_badarg(env);
        }
        value = binary.data;
        value_length = @intCast(binary.size);
    }
    return handleResult(env, fdb.fdb_network_set_option(option, value, value_length));
}

fn setupNetwork(env: ?*erl.ErlNifEnv, argc: c_int, _: [*c]const erl.ERL_NIF_TERM) callconv(.c) erl.ERL_NIF_TERM {
    if (argc != 0) {
        return erl.enif_make_badarg(env);
    }
    return handleResult(env, fdb.fdb_setup_network());
}

const State = struct {
    var network_err: c_int = 0;
    var network_tid: ?*erl.ErlNifTid = null;
};

fn networkFunc(err_ptr: ?*anyopaque) callconv(.c) ?*anyopaque {
    const err = @as(*c_int, @ptrCast(@alignCast(err_ptr.?)));
    err.* = fdb.fdb_run_network();
    return null;
}

fn runNetwork(env: ?*erl.ErlNifEnv, argc: c_int, _: [*c]const erl.ERL_NIF_TERM) callconv(.c) erl.ERL_NIF_TERM {
    const name = @constCast("fdb".ptr);
    if (argc != 0) {
        return erl.enif_make_badarg(env);
    }
    State.network_tid = @ptrCast(@alignCast(erl.enif_alloc(@sizeOf(erl.ErlNifTid))));
    if (State.network_tid == null) {
        return erl.enif_raise_exception(env, Atoms.ENOMEM);
    }
    const err = erl.enif_thread_create(name, State.network_tid, &networkFunc, @ptrCast(@constCast(&State.network_err)), null);
    if (err != 0) {
        const atom_name = erl.erl_errno_id(err);
        const term = erl.enif_make_atom(env, atom_name);
        return erl.enif_make_tuple2(env, Atoms.ERROR, term);
    }
    return Atoms.OK;
}

fn stopNetwork(env: ?*erl.ErlNifEnv, argc: c_int, _: [*c]const erl.ERL_NIF_TERM) callconv(.c) erl.ERL_NIF_TERM {
    if (argc != 0) {
        return erl.enif_make_badarg(env);
    }
    var err: c_int = fdb.fdb_stop_network();
    if (err != 0) {
        return handleError(env, err);
    }
    if (State.network_tid != null) {
        err = erl.enif_thread_join(State.network_tid.?.*, null);
        if (err != 0) {
            const atom_name = erl.erl_errno_id(err);
            const term = erl.enif_make_atom(env, atom_name);
            return erl.enif_make_tuple2(env, Atoms.ERROR, term);
        }
    }
    return Atoms.OK;
}

const FutureType = enum {
    void,
    double,
    int64,
    key,
    key_array,
    keyvalue_array,
    string_array,
    value,
};
const Future = struct {
    handle: *fdb.FDBFuture,
    resource: ?*anyopaque,
    type: FutureType,

    pub fn get(self: *Future, env: ?*erl.ErlNifEnv) erl.ERL_NIF_TERM {
        const err: c_int = fdb.fdb_future_get_error(self.handle);
        if (err != 0) {
            return handleError(env, err);
        }
        switch (self.type) {
            .void => return erl.enif_make_tuple2(env, Atoms.OK, Atoms.NIL),
            .double => return self.getDouble(env),
            .int64 => return self.getInt64(env),
            .key => return self.getKey(env),
            .key_array => return self.getKeyArray(env),
            .keyvalue_array => return self.getKeyvalueArray(env),
            .string_array => return self.getStringArray(env),
            .value => return self.getValue(env),
        }
    }

    fn getInt64(self: *Future, env: ?*erl.ErlNifEnv) erl.ERL_NIF_TERM {
        var value: i64 = 0;
        const err: c_int = fdb.fdb_future_get_int64(self.handle, &value);
        if (err != 0) {
            return handleError(env, err);
        }
        return erl.enif_make_tuple2(env, Atoms.OK, erl.enif_make_int64(env, value));
    }

    fn getDouble(self: *Future, env: ?*erl.ErlNifEnv) erl.ERL_NIF_TERM {
        var value: f64 = 0;
        const err: c_int = fdb.fdb_future_get_double(self.handle, &value);
        if (err != 0) {
            return handleError(env, err);
        }
        return erl.enif_make_tuple2(env, Atoms.OK, erl.enif_make_double(env, value));
    }

    fn getKeyArray(self: *Future, env: ?*erl.ErlNifEnv) erl.ERL_NIF_TERM {
        // HACK: Zig is not parsing the type `fdb.FDBKey` correctly due to
        // the #pragma pack 4 so we have to create the struct ourselves.
        var keys: [*]struct {
            key: *const u8 align(4),
            key_length: c_int align(4),
        } = undefined;
        var count: c_int = undefined;
        const err: c_int = fdb.fdb_future_get_key_array(self.handle, @ptrCast(&keys), &count);
        if (err != 0) {
            return handleError(env, err);
        }
        var list = erl.enif_make_list(env, 0);
        var i: usize = 0;
        while (i < count) : (i += 1) {
            const key = keys[i];
            const term = erl.enif_make_resource_binary(env, self, key.key, @intCast(key.key_length));
            list = erl.enif_make_list_cell(env, term, list);
        }
        return erl.enif_make_tuple2(env, Atoms.OK, list);
    }

    fn getKey(self: *Future, env: ?*erl.ErlNifEnv) erl.ERL_NIF_TERM {
        var key: ?[*]u8 = null;
        var key_length: c_int = 0;
        const err: c_int = fdb.fdb_future_get_key(self.handle, @ptrCast(&key), &key_length);
        if (err != 0) {
            return handleError(env, err);
        }
        const bin = erl.enif_make_resource_binary(env, self, key, @intCast(key_length));
        return erl.enif_make_tuple2(env, Atoms.OK, bin);
    }

    fn getValue(self: *Future, env: ?*erl.ErlNifEnv) erl.ERL_NIF_TERM {
        var present: fdb.fdb_bool_t = undefined;
        var value: ?[*]u8 = null;
        var value_length: c_int = 0;
        const err: c_int = fdb.fdb_future_get_value(self.handle, &present, @ptrCast(&value), &value_length);
        if (err != 0) {
            return handleError(env, err);
        }
        if (present != 0) {
            const bin = erl.enif_make_resource_binary(env, self, value, @intCast(value_length));
            return erl.enif_make_tuple2(env, Atoms.OK, bin);
        }
        return erl.enif_make_tuple2(env, Atoms.OK, Atoms.NIL);
    }

    fn getStringArray(self: *Future, env: ?*erl.ErlNifEnv) erl.ERL_NIF_TERM {
        var strings: ?[*][*c]u8 = null;
        var count: c_int = 0;
        const err: c_int = fdb.fdb_future_get_string_array(self.handle, &strings, &count);
        if (err != 0) {
            return handleError(env, err);
        }
        var list = erl.enif_make_list(env, 0);
        var i: usize = 0;
        while (i < count) : (i += 1) {
            const string: [*c]u8 = strings.?[i];
            const term = erl.enif_make_resource_binary(env, self, string, @intCast(std.mem.len(string)));
            list = erl.enif_make_list_cell(env, term, list);
        }
        return erl.enif_make_tuple2(env, Atoms.OK, list);
    }

    fn getKeyvalueArray(self: *Future, env: ?*erl.ErlNifEnv) erl.ERL_NIF_TERM {
        // HACK: Zig is not parsing the type `fdb.FDBKeyValue` correctly due to
        // the #pragma pack 4 so we have to create the struct ourselves.
        var kvs: [*]struct {
            key: *const u8 align(4),
            key_length: c_int align(4),
            value: *const u8 align(4),
            value_length: c_int align(4),
        } = undefined;
        var count: c_int = undefined;
        var more: fdb.fdb_bool_t = undefined;
        const err: c_int = fdb.fdb_future_get_keyvalue_array(self.handle, @ptrCast(@alignCast(&kvs)), &count, &more);
        if (err != 0) {
            return handleError(env, err);
        }
        var list = erl.enif_make_list(env, 0);
        var i: usize = 0;
        while (i < count) : (i += 1) {
            const kv = kvs[i];
            const k = erl.enif_make_resource_binary(env, self, kv.key, @intCast(kv.key_length));
            const v = erl.enif_make_resource_binary(env, self, kv.value, @intCast(kv.value_length));
            const term = erl.enif_make_tuple2(env, k, v);
            list = erl.enif_make_list_cell(env, term, list);
        }
        return erl.enif_make_tuple3(env, Atoms.OK, erl.enif_make_int(env, more), list);
    }
};

const Callback = struct {
    env: ?*erl.ErlNifEnv,
    pid: erl.ErlNifPid,
    ref: erl.ERL_NIF_TERM,
    future: *Future,
};

fn futureCallback(future: ?*fdb.FDBFuture, arg: ?*anyopaque) callconv(.c) void {
    const callback = @as(?*Callback, @ptrCast(@alignCast(arg)));
    defer erl.enif_free(callback);
    defer erl.enif_free_env(callback.?.env);
    defer erl.enif_release_resource(callback.?.future);
    if (callback.?.future.handle != future) {
        @panic("Futures do not match!");
    }
    const result = callback.?.future.get(callback.?.env);
    const msg = erl.enif_make_tuple2(callback.?.env, callback.?.ref, result);
    const sent: c_int = erl.enif_send(null, &(callback.?.pid), callback.?.env, msg);
    if (sent == 0) {
        // An error here just means the message failed to send, either the sender or receiver are dead.
    }
    return;
}

fn futureResolve(env: ?*erl.ErlNifEnv, argc: c_int, argv: [*c]const erl.ERL_NIF_TERM) callconv(.c) erl.ERL_NIF_TERM {
    var future: *Future = undefined;
    if (argc != 2) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_resource(env, argv[0], Resources.FUTURE, @ptrCast(&future)) == 0) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_is_ref(env, argv[1]) == 0) {
        return erl.enif_make_badarg(env);
    }
    const arg: ?*Callback = @ptrCast(@alignCast(erl.enif_alloc(@sizeOf(Callback))));
    if (arg == null) {
        return erl.enif_raise_exception(env, Atoms.ENOMEM);
    }
    arg.?.env = erl.enif_alloc_env();
    if (arg.?.env == null) {
        return erl.enif_raise_exception(env, Atoms.ENOMEM);
    }
    const pid = erl.enif_self(env, &arg.?.pid);
    if (pid == null) {
        return erl.enif_raise_exception(env, Atoms.ENOMEM);
    }
    arg.?.future = future;
    arg.?.ref = erl.enif_make_copy(arg.?.env, argv[1]);
    erl.enif_keep_resource(future);
    const err: c_int = fdb.fdb_future_set_callback(future.handle, &futureCallback, @ptrCast(@constCast(arg)));
    return handleResult(env, err);
}

fn futureCancel(env: ?*erl.ErlNifEnv, argc: c_int, argv: [*c]const erl.ERL_NIF_TERM) callconv(.c) erl.ERL_NIF_TERM {
    var future: ?*Future = null;
    if (argc != 1) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_resource(env, argv[0], Resources.FUTURE, @ptrCast(&future)) == 0) {
        return erl.enif_make_badarg(env);
    }
    fdb.fdb_future_cancel(future.?.handle);
    return Atoms.OK;
}

fn futureIsReady(env: ?*erl.ErlNifEnv, argc: c_int, argv: [*c]const erl.ERL_NIF_TERM) callconv(.c) erl.ERL_NIF_TERM {
    var future: ?*Future = null;
    if (argc != 1) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_resource(env, argv[0], Resources.FUTURE, @ptrCast(&future)) == 0) {
        return erl.enif_make_badarg(env);
    }
    const err: c_int = fdb.fdb_future_is_ready(future.?.handle);
    return handleResult(env, err);
}

fn createDatabase(env: ?*erl.ErlNifEnv, argc: c_int, argv: [*c]const erl.ERL_NIF_TERM) callconv(.c) erl.ERL_NIF_TERM {
    var cluster_file_path: ?[*:0]u8 = null;
    defer if (cluster_file_path) |path| erl.enif_free(path);
    var path_bin: erl.ErlNifBinary = undefined;
    if (argc > 1) {
        return erl.enif_make_badarg(env);
    }
    if (argc == 1 and argv[0] != Atoms.NIL) {
        if (erl.enif_inspect_binary(env, argv[0], &path_bin) == 0) {
            return erl.enif_make_badarg(env);
        }
        cluster_file_path = @ptrCast(@alignCast(erl.enif_alloc(@sizeOf(u8) * (path_bin.size + 1))));
        @memcpy(cluster_file_path.?[0..path_bin.size], path_bin.data[0..path_bin.size]);
        cluster_file_path.?[path_bin.size] = 0;
    }
    var database: ?*fdb.FDBDatabase = null;
    const err: c_int = fdb.fdb_create_database(cluster_file_path, &database);
    if (err != 0) {
        return handleError(env, err);
    }
    const db: **fdb.FDBDatabase = @ptrCast(@alignCast(erl.enif_alloc_resource(Resources.FDB_DATABASE, @sizeOf(*fdb.FDBDatabase))));
    db.* = database.?;
    const term: erl.ERL_NIF_TERM = erl.enif_make_resource(env, @ptrCast(@alignCast(db)));
    erl.enif_release_resource(@ptrCast(@alignCast(db)));
    return erl.enif_make_tuple2(env, Atoms.OK, term);
}

fn databaseSetOption(env: ?*erl.ErlNifEnv, argc: c_int, argv: [*c]const erl.ERL_NIF_TERM) callconv(.c) erl.ERL_NIF_TERM {
    var database: **fdb.FDBDatabase = undefined;
    var option: fdb.FDBDatabaseOption = undefined;
    var value: ?*u8 = null;
    var value_length: c_int = 0;
    if (argc < 2 or argc > 3) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_resource(env, argv[0], Resources.FDB_DATABASE, @ptrCast(&database)) == 0) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_uint(env, argv[1], &option) == 0) {
        return erl.enif_make_badarg(env);
    }
    if (argc == 3) {
        var binary: erl.ErlNifBinary = undefined;
        if (erl.enif_inspect_binary(env, argv[2], &binary) == 0) {
            return erl.enif_make_badarg(env);
        }
        value = binary.data;
        value_length = @intCast(binary.size);
    }
    const err: c_int = fdb.fdb_database_set_option(database.*, option, value, value_length);
    return handleResult(env, err);
}

fn databaseOpenTenant(env: ?*erl.ErlNifEnv, argc: c_int, argv: [*c]const erl.ERL_NIF_TERM) callconv(.c) erl.ERL_NIF_TERM {
    var database: **fdb.FDBDatabase = undefined;
    var tenant: ?*fdb.FDBTenant = null;
    var tenant_name: ?*u8 = null;
    var tenant_name_length: c_int = 0;
    if (argc != 2) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_resource(env, argv[0], Resources.FDB_DATABASE, @ptrCast(&database)) == 0) {
        return erl.enif_make_badarg(env);
    }
    var binary: erl.ErlNifBinary = undefined;
    if (erl.enif_inspect_binary(env, argv[1], &binary) == 0) {
        return erl.enif_make_badarg(env);
    }
    tenant_name = binary.data;
    tenant_name_length = @intCast(binary.size);
    const err: c_int = fdb.fdb_database_open_tenant(database.*, tenant_name, tenant_name_length, &tenant);
    if (err != 0) {
        return handleError(env, err);
    }
    const tn: **fdb.FDBTenant = @ptrCast(@alignCast(erl.enif_alloc_resource(Resources.FDB_TENANT, @sizeOf(*fdb.FDBTenant))));
    tn.* = tenant.?;
    const term: erl.ERL_NIF_TERM = erl.enif_make_resource(env, @ptrCast(@alignCast(tn)));
    erl.enif_release_resource(@ptrCast(@alignCast(tn)));
    return erl.enif_make_tuple2(env, Atoms.OK, term);
}

fn databaseCreateTransaction(env: ?*erl.ErlNifEnv, argc: c_int, argv: [*c]const erl.ERL_NIF_TERM) callconv(.c) erl.ERL_NIF_TERM {
    var database: **fdb.FDBDatabase = undefined;
    var transaction: ?*fdb.FDBTransaction = null;
    if (argc != 1) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_resource(env, argv[0], Resources.FDB_DATABASE, @ptrCast(&database)) == 0) {
        return erl.enif_make_badarg(env);
    }
    const err: c_int = fdb.fdb_database_create_transaction(database.*, &transaction);
    if (err != 0) {
        return handleError(env, err);
    }
    const tx: *Transaction = @ptrCast(@alignCast(erl.enif_alloc_resource(Resources.TRANSACTION, @sizeOf(Transaction))));
    tx.handle = transaction.?;
    tx.resource = @ptrCast(database);
    erl.enif_keep_resource(tx.resource);
    const term: erl.ERL_NIF_TERM = erl.enif_make_resource(env, @ptrCast(@alignCast(tx)));
    erl.enif_release_resource(@ptrCast(@alignCast(tx)));
    return erl.enif_make_tuple2(env, Atoms.OK, term);
}

fn databaseRebootWorker(env: ?*erl.ErlNifEnv, argc: c_int, argv: [*c]const erl.ERL_NIF_TERM) callconv(.c) erl.ERL_NIF_TERM {
    var database: **fdb.FDBDatabase = undefined;
    var address: ?*u8 = null;
    var address_length: c_int = 0;
    var check: c_int = undefined;
    var duration: c_int = undefined;
    if (argc != 4) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_resource(env, argv[0], Resources.FDB_DATABASE, @ptrCast(&database)) == 0) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_int(env, argv[2], &check) == 0) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_int(env, argv[3], &duration) == 0) {
        return erl.enif_make_badarg(env);
    }
    var binary: erl.ErlNifBinary = undefined;
    if (erl.enif_inspect_binary(env, argv[1], &binary) == 0) {
        return erl.enif_make_badarg(env);
    }
    address = binary.data;
    address_length = @intCast(binary.size);
    const future: ?*fdb.FDBFuture = fdb.fdb_database_reboot_worker(database.*, address, address_length, check, duration);
    const ft: *Future = @ptrCast(@alignCast(erl.enif_alloc_resource(Resources.FUTURE, @sizeOf(Future))));
    ft.handle = future.?;
    ft.resource = @ptrCast(database);
    ft.type = .int64;
    erl.enif_keep_resource(ft.resource);
    const term: erl.ERL_NIF_TERM = erl.enif_make_resource(env, @ptrCast(@alignCast(ft)));
    erl.enif_release_resource(@ptrCast(@alignCast(ft)));
    return term;
}

fn databaseForceRecoveryWithDataLoss(env: ?*erl.ErlNifEnv, argc: c_int, argv: [*c]const erl.ERL_NIF_TERM) callconv(.c) erl.ERL_NIF_TERM {
    var database: **fdb.FDBDatabase = undefined;
    var dc_id: ?*u8 = null;
    var dc_id_length: c_int = 0;
    if (argc != 2) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_resource(env, argv[0], Resources.FDB_DATABASE, @ptrCast(&database)) == 0) {
        return erl.enif_make_badarg(env);
    }
    var binary: erl.ErlNifBinary = undefined;
    if (erl.enif_inspect_binary(env, argv[1], &binary) == 0) {
        return erl.enif_make_badarg(env);
    }
    dc_id = binary.data;
    dc_id_length = @intCast(binary.size);
    const future: ?*fdb.FDBFuture = fdb.fdb_database_force_recovery_with_data_loss(database.*, dc_id, dc_id_length);
    const ft: *Future = @ptrCast(@alignCast(erl.enif_alloc_resource(Resources.FUTURE, @sizeOf(Future))));
    ft.handle = future.?;
    ft.resource = @ptrCast(database);
    ft.type = .void;
    erl.enif_keep_resource(ft.resource);
    const term: erl.ERL_NIF_TERM = erl.enif_make_resource(env, @ptrCast(@alignCast(ft)));
    erl.enif_release_resource(@ptrCast(@alignCast(ft)));
    return term;
}

fn databaseCreateSnapshot(env: ?*erl.ErlNifEnv, argc: c_int, argv: [*c]const erl.ERL_NIF_TERM) callconv(.c) erl.ERL_NIF_TERM {
    var database: **fdb.FDBDatabase = undefined;
    var uid: ?*u8 = null;
    var uid_length: c_int = 0;
    var snapshot_command: ?*u8 = null;
    var snapshot_command_length: c_int = 0;
    if (argc != 3) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_resource(env, argv[0], Resources.FDB_DATABASE, @ptrCast(&database)) == 0) {
        return erl.enif_make_badarg(env);
    }
    var uid_binary: erl.ErlNifBinary = undefined;
    if (erl.enif_inspect_binary(env, argv[1], &uid_binary) == 0) {
        return erl.enif_make_badarg(env);
    }
    uid = uid_binary.data;
    uid_length = @intCast(uid_binary.size);
    var snapshot_binary: erl.ErlNifBinary = undefined;
    if (erl.enif_inspect_binary(env, argv[1], &snapshot_binary) == 0) {
        return erl.enif_make_badarg(env);
    }
    snapshot_command = snapshot_binary.data;
    snapshot_command_length = @intCast(snapshot_binary.size);
    const future: ?*fdb.FDBFuture = fdb.fdb_database_create_snapshot(database.*, uid, uid_length, snapshot_command, snapshot_command_length);
    const ft: *Future = @ptrCast(@alignCast(erl.enif_alloc_resource(Resources.FUTURE, @sizeOf(Future))));
    ft.handle = future.?;
    ft.resource = @ptrCast(database);
    ft.type = .void;
    erl.enif_keep_resource(ft.resource);
    const term: erl.ERL_NIF_TERM = erl.enif_make_resource(env, @ptrCast(@alignCast(ft)));
    erl.enif_release_resource(@ptrCast(@alignCast(ft)));
    return term;
}

fn databaseGetMainThreadBusyness(env: ?*erl.ErlNifEnv, argc: c_int, argv: [*c]const erl.ERL_NIF_TERM) callconv(.c) erl.ERL_NIF_TERM {
    var database: **fdb.FDBDatabase = undefined;
    if (argc != 1) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_resource(env, argv[0], Resources.FDB_DATABASE, @ptrCast(&database)) == 0) {
        return erl.enif_make_badarg(env);
    }
    const double: f64 = fdb.fdb_database_get_main_thread_busyness(database.*);
    return erl.enif_make_double(env, double);
}

fn databaseGetClientStatus(env: ?*erl.ErlNifEnv, argc: c_int, argv: [*c]const erl.ERL_NIF_TERM) callconv(.c) erl.ERL_NIF_TERM {
    var database: **fdb.FDBDatabase = undefined;
    if (argc != 1) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_resource(env, argv[0], Resources.FDB_DATABASE, @ptrCast(&database)) == 0) {
        return erl.enif_make_badarg(env);
    }
    const future: ?*fdb.FDBFuture = fdb.fdb_database_get_client_status(database.*);
    const ft: *Future = @ptrCast(@alignCast(erl.enif_alloc_resource(Resources.FUTURE, @sizeOf(Future))));
    ft.handle = future.?;
    ft.resource = @ptrCast(database);
    ft.type = .key;
    erl.enif_keep_resource(ft.resource);
    const term: erl.ERL_NIF_TERM = erl.enif_make_resource(env, @ptrCast(@alignCast(ft)));
    erl.enif_release_resource(@ptrCast(@alignCast(ft)));
    return term;
}

fn tenantCreateTransaction(env: ?*erl.ErlNifEnv, argc: c_int, argv: [*c]const erl.ERL_NIF_TERM) callconv(.c) erl.ERL_NIF_TERM {
    var tenant: **fdb.FDBTenant = undefined;
    var transaction: ?*fdb.FDBTransaction = null;
    if (argc != 1) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_resource(env, argv[0], Resources.FDB_TENANT, @ptrCast(&tenant)) == 0) {
        return erl.enif_make_badarg(env);
    }
    const err: c_int = fdb.fdb_tenant_create_transaction(tenant.*, &transaction);
    if (err != 0) {
        return handleError(env, err);
    }
    const tx: *Transaction = @ptrCast(@alignCast(erl.enif_alloc_resource(Resources.TRANSACTION, @sizeOf(Transaction))));
    tx.handle = transaction.?;
    tx.resource = @ptrCast(tenant);
    erl.enif_keep_resource(tx.resource);
    const term: erl.ERL_NIF_TERM = erl.enif_make_resource(env, @ptrCast(@alignCast(tx)));
    erl.enif_release_resource(@ptrCast(@alignCast(tx)));
    return erl.enif_make_tuple2(env, Atoms.OK, term);
}

fn tenantGetId(env: ?*erl.ErlNifEnv, argc: c_int, argv: [*c]const erl.ERL_NIF_TERM) callconv(.c) erl.ERL_NIF_TERM {
    var tenant: **fdb.FDBTenant = undefined;
    if (argc != 1) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_resource(env, argv[0], Resources.FDB_TENANT, @ptrCast(&tenant)) == 0) {
        return erl.enif_make_badarg(env);
    }
    const future: ?*fdb.FDBFuture = fdb.fdb_tenant_get_id(tenant.*);
    const ft: *Future = @ptrCast(@alignCast(erl.enif_alloc_resource(Resources.FUTURE, @sizeOf(Future))));
    ft.handle = future.?;
    ft.resource = @ptrCast(tenant);
    ft.type = .int64;
    erl.enif_keep_resource(ft.resource);
    const term: erl.ERL_NIF_TERM = erl.enif_make_resource(env, @ptrCast(@alignCast(ft)));
    erl.enif_release_resource(@ptrCast(@alignCast(ft)));
    return term;
}

const Transaction = struct {
    handle: *fdb.FDBTransaction,
    resource: ?*anyopaque,
};

fn transactionSetOption(env: ?*erl.ErlNifEnv, argc: c_int, argv: [*c]const erl.ERL_NIF_TERM) callconv(.c) erl.ERL_NIF_TERM {
    var transaction: *Transaction = undefined;
    var option: fdb.FDBTransactionOption = undefined;
    var value: ?*u8 = null;
    var value_length: c_int = 0;
    if (argc < 2 or argc > 3) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_resource(env, argv[0], Resources.TRANSACTION, @ptrCast(&transaction)) == 0) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_uint(env, argv[1], &option) == 0) {
        return erl.enif_make_badarg(env);
    }
    if (argc == 3) {
        var binary: erl.ErlNifBinary = undefined;
        if (erl.enif_inspect_binary(env, argv[2], &binary) == 0) {
            return erl.enif_make_badarg(env);
        }
        value = binary.data;
        value_length = @intCast(binary.size);
    }
    const err: c_int = fdb.fdb_transaction_set_option(transaction.handle, option, value, value_length);
    return handleResult(env, err);
}

fn transactionSetReadVersion(env: ?*erl.ErlNifEnv, argc: c_int, argv: [*c]const erl.ERL_NIF_TERM) callconv(.c) erl.ERL_NIF_TERM {
    var transaction: *Transaction = undefined;
    var version: i64 = undefined;
    if (argc != 2) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_resource(env, argv[0], Resources.TRANSACTION, @ptrCast(&transaction)) == 0) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_int64(env, argv[1], &version) == 0) {
        return erl.enif_make_badarg(env);
    }
    fdb.fdb_transaction_set_read_version(transaction.handle, version);
    return Atoms.OK;
}

fn transactionGetReadVersion(env: ?*erl.ErlNifEnv, argc: c_int, argv: [*c]const erl.ERL_NIF_TERM) callconv(.c) erl.ERL_NIF_TERM {
    var transaction: *Transaction = undefined;
    if (argc != 1) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_resource(env, argv[0], Resources.TRANSACTION, @ptrCast(&transaction)) == 0) {
        return erl.enif_make_badarg(env);
    }
    const future: ?*fdb.FDBFuture = fdb.fdb_transaction_get_read_version(transaction.handle);
    const ft: *Future = @ptrCast(@alignCast(erl.enif_alloc_resource(Resources.FUTURE, @sizeOf(Future))));
    ft.handle = future.?;
    ft.resource = @ptrCast(transaction);
    ft.type = .int64;
    erl.enif_keep_resource(ft.resource);
    const term: erl.ERL_NIF_TERM = erl.enif_make_resource(env, @ptrCast(@alignCast(ft)));
    erl.enif_release_resource(@ptrCast(@alignCast(ft)));
    return term;
}

fn transactionGet(env: ?*erl.ErlNifEnv, argc: c_int, argv: [*c]const erl.ERL_NIF_TERM) callconv(.c) erl.ERL_NIF_TERM {
    var transaction: *Transaction = undefined;
    var key_name: ?*u8 = null;
    var key_name_length: c_int = 0;
    var snapshot: c_int = undefined;
    if (argc != 3) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_resource(env, argv[0], Resources.TRANSACTION, @ptrCast(&transaction)) == 0) {
        return erl.enif_make_badarg(env);
    }
    var binary: erl.ErlNifBinary = undefined;
    if (erl.enif_inspect_binary(env, argv[1], &binary) == 0) {
        return erl.enif_make_badarg(env);
    }
    key_name = binary.data;
    key_name_length = @intCast(binary.size);
    if (erl.enif_get_int(env, argv[2], &snapshot) == 0) {
        return erl.enif_make_badarg(env);
    }
    const future: ?*fdb.FDBFuture = fdb.fdb_transaction_get(transaction.handle, key_name, key_name_length, snapshot);
    const ft: *Future = @ptrCast(@alignCast(erl.enif_alloc_resource(Resources.FUTURE, @sizeOf(Future))));
    ft.handle = future.?;
    ft.resource = @ptrCast(transaction);
    ft.type = .value;
    erl.enif_keep_resource(ft.resource);
    const term: erl.ERL_NIF_TERM = erl.enif_make_resource(env, @ptrCast(@alignCast(ft)));
    erl.enif_release_resource(@ptrCast(@alignCast(ft)));
    return term;
}

fn transactionGetEstimatedRangeSizeBytes(env: ?*erl.ErlNifEnv, argc: c_int, argv: [*c]const erl.ERL_NIF_TERM) callconv(.c) erl.ERL_NIF_TERM {
    var transaction: *Transaction = undefined;
    var begin_key_name: ?*u8 = null;
    var begin_key_name_length: c_int = 0;
    var end_key_name: ?*u8 = null;
    var end_key_name_length: c_int = 0;
    if (argc != 3) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_resource(env, argv[0], Resources.TRANSACTION, @ptrCast(&transaction)) == 0) {
        return erl.enif_make_badarg(env);
    }
    var begin_key_binary: erl.ErlNifBinary = undefined;
    if (erl.enif_inspect_binary(env, argv[1], &begin_key_binary) == 0) {
        return erl.enif_make_badarg(env);
    }
    begin_key_name = begin_key_binary.data;
    begin_key_name_length = @intCast(begin_key_binary.size);
    var end_key_binary: erl.ErlNifBinary = undefined;
    if (erl.enif_inspect_binary(env, argv[2], &end_key_binary) == 0) {
        return erl.enif_make_badarg(env);
    }
    end_key_name = end_key_binary.data;
    end_key_name_length = @intCast(end_key_binary.size);
    const future: ?*fdb.FDBFuture = fdb.fdb_transaction_get_estimated_range_size_bytes(transaction.handle, begin_key_name, begin_key_name_length, end_key_name, end_key_name_length);
    const ft: *Future = @ptrCast(@alignCast(erl.enif_alloc_resource(Resources.FUTURE, @sizeOf(Future))));
    ft.handle = future.?;
    ft.resource = @ptrCast(transaction);
    ft.type = .int64;
    erl.enif_keep_resource(ft.resource);
    const term: erl.ERL_NIF_TERM = erl.enif_make_resource(env, @ptrCast(@alignCast(ft)));
    erl.enif_release_resource(@ptrCast(@alignCast(ft)));
    return term;
}

fn transactionGetRangeSplitPoints(env: ?*erl.ErlNifEnv, argc: c_int, argv: [*c]const erl.ERL_NIF_TERM) callconv(.c) erl.ERL_NIF_TERM {
    var transaction: *Transaction = undefined;
    var begin_key_name: ?*u8 = null;
    var begin_key_name_length: c_int = 0;
    var end_key_name: ?*u8 = null;
    var end_key_name_length: c_int = 0;
    var chunk_size: i64 = undefined;
    if (argc != 4) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_resource(env, argv[0], Resources.TRANSACTION, @ptrCast(&transaction)) == 0) {
        return erl.enif_make_badarg(env);
    }
    var begin_key_binary: erl.ErlNifBinary = undefined;
    if (erl.enif_inspect_binary(env, argv[1], &begin_key_binary) == 0) {
        return erl.enif_make_badarg(env);
    }
    begin_key_name = begin_key_binary.data;
    begin_key_name_length = @intCast(begin_key_binary.size);
    var end_key_binary: erl.ErlNifBinary = undefined;
    if (erl.enif_inspect_binary(env, argv[2], &end_key_binary) == 0) {
        return erl.enif_make_badarg(env);
    }
    end_key_name = end_key_binary.data;
    end_key_name_length = @intCast(end_key_binary.size);
    if (erl.enif_get_int64(env, argv[3], &chunk_size) == 0) {
        return erl.enif_make_badarg(env);
    }
    const future: ?*fdb.FDBFuture = fdb.fdb_transaction_get_range_split_points(transaction.handle, begin_key_name, begin_key_name_length, end_key_name, end_key_name_length, chunk_size);
    const ft: *Future = @ptrCast(@alignCast(erl.enif_alloc_resource(Resources.FUTURE, @sizeOf(Future))));
    ft.handle = future.?;
    ft.resource = @ptrCast(transaction);
    ft.type = .key_array;
    erl.enif_keep_resource(ft.resource);
    const term: erl.ERL_NIF_TERM = erl.enif_make_resource(env, @ptrCast(@alignCast(ft)));
    erl.enif_release_resource(@ptrCast(@alignCast(ft)));
    return term;
}

fn transactionGetKey(env: ?*erl.ErlNifEnv, argc: c_int, argv: [*c]const erl.ERL_NIF_TERM) callconv(.c) erl.ERL_NIF_TERM {
    var transaction: *Transaction = undefined;
    var key_name: ?*u8 = null;
    var key_name_length: c_int = 0;
    var or_equal: c_int = undefined;
    var offset: c_int = undefined;
    var snapshot: c_int = undefined;
    if (argc != 5) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_resource(env, argv[0], Resources.TRANSACTION, @ptrCast(&transaction)) == 0) {
        return erl.enif_make_badarg(env);
    }
    var key_binary: erl.ErlNifBinary = undefined;
    if (erl.enif_inspect_binary(env, argv[1], &key_binary) == 0) {
        return erl.enif_make_badarg(env);
    }
    key_name = key_binary.data;
    key_name_length = @intCast(key_binary.size);
    if (erl.enif_get_int(env, argv[2], &or_equal) == 0) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_int(env, argv[3], &offset) == 0) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_int(env, argv[4], &snapshot) == 0) {
        return erl.enif_make_badarg(env);
    }
    const future: ?*fdb.FDBFuture = fdb.fdb_transaction_get_key(transaction.handle, key_name, key_name_length, or_equal, offset, snapshot);
    const ft: *Future = @ptrCast(@alignCast(erl.enif_alloc_resource(Resources.FUTURE, @sizeOf(Future))));
    ft.handle = future.?;
    ft.resource = @ptrCast(transaction);
    ft.type = .key;
    erl.enif_keep_resource(ft.resource);
    const term: erl.ERL_NIF_TERM = erl.enif_make_resource(env, @ptrCast(@alignCast(ft)));
    erl.enif_release_resource(@ptrCast(@alignCast(ft)));
    return term;
}

fn transactionGetAddressesForKey(env: ?*erl.ErlNifEnv, argc: c_int, argv: [*c]const erl.ERL_NIF_TERM) callconv(.c) erl.ERL_NIF_TERM {
    var transaction: *Transaction = undefined;
    var key_name: ?*u8 = null;
    var key_name_length: c_int = 0;
    if (argc != 2) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_resource(env, argv[0], Resources.TRANSACTION, @ptrCast(&transaction)) == 0) {
        return erl.enif_make_badarg(env);
    }
    var key_binary: erl.ErlNifBinary = undefined;
    if (erl.enif_inspect_binary(env, argv[1], &key_binary) == 0) {
        return erl.enif_make_badarg(env);
    }
    key_name = key_binary.data;
    key_name_length = @intCast(key_binary.size);
    const future: ?*fdb.FDBFuture = fdb.fdb_transaction_get_addresses_for_key(transaction.handle, key_name, key_name_length);
    const ft: *Future = @ptrCast(@alignCast(erl.enif_alloc_resource(Resources.FUTURE, @sizeOf(Future))));
    ft.handle = future.?;
    ft.resource = @ptrCast(transaction);
    ft.type = .string_array;
    erl.enif_keep_resource(ft.resource);
    const term: erl.ERL_NIF_TERM = erl.enif_make_resource(env, @ptrCast(@alignCast(ft)));
    erl.enif_release_resource(@ptrCast(@alignCast(ft)));
    return term;
}

fn transactionGetRange(env: ?*erl.ErlNifEnv, argc: c_int, argv: [*c]const erl.ERL_NIF_TERM) callconv(.c) erl.ERL_NIF_TERM {
    var transaction: *Transaction = undefined;
    var begin_key_name: ?*u8 = null;
    var begin_key_name_length: c_int = 0;
    var begin_or_equal: c_int = undefined;
    var begin_offset: c_int = undefined;
    var end_key_name: ?*u8 = null;
    var end_key_name_length: c_int = 0;
    var end_or_equal: c_int = undefined;
    var end_offset: c_int = undefined;
    var limit: c_int = undefined;
    var target_bytes: c_int = undefined;
    var mode: c_int = undefined;
    var iteration: c_int = undefined;
    var snapshot: c_int = undefined;
    var reverse: c_int = undefined;
    if (argc != 13) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_resource(env, argv[0], Resources.TRANSACTION, @ptrCast(&transaction)) == 0) {
        return erl.enif_make_badarg(env);
    }
    var begin_key_binary: erl.ErlNifBinary = undefined;
    if (erl.enif_inspect_binary(env, argv[1], &begin_key_binary) == 0) {
        return erl.enif_make_badarg(env);
    }
    begin_key_name = begin_key_binary.data;
    begin_key_name_length = @intCast(begin_key_binary.size);
    if (erl.enif_get_int(env, argv[2], &begin_or_equal) == 0) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_int(env, argv[3], &begin_offset) == 0) {
        return erl.enif_make_badarg(env);
    }
    var end_key_binary: erl.ErlNifBinary = undefined;
    if (erl.enif_inspect_binary(env, argv[4], &end_key_binary) == 0) {
        return erl.enif_make_badarg(env);
    }
    end_key_name = end_key_binary.data;
    end_key_name_length = @intCast(end_key_binary.size);
    if (erl.enif_get_int(env, argv[5], &end_or_equal) == 0) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_int(env, argv[6], &end_offset) == 0) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_int(env, argv[7], &limit) == 0) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_int(env, argv[8], &target_bytes) == 0) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_int(env, argv[9], &mode) == 0) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_int(env, argv[10], &iteration) == 0) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_int(env, argv[11], &snapshot) == 0) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_int(env, argv[12], &reverse) == 0) {
        return erl.enif_make_badarg(env);
    }
    const future: ?*fdb.FDBFuture = fdb.fdb_transaction_get_range(transaction.handle, begin_key_name, begin_key_name_length, begin_or_equal, begin_offset, end_key_name, end_key_name_length, end_or_equal, end_offset, limit, target_bytes, mode, iteration, snapshot, reverse);
    const ft: *Future = @ptrCast(@alignCast(erl.enif_alloc_resource(Resources.FUTURE, @sizeOf(Future))));
    ft.handle = future.?;
    ft.resource = @ptrCast(transaction);
    ft.type = .keyvalue_array;
    erl.enif_keep_resource(ft.resource);
    const term: erl.ERL_NIF_TERM = erl.enif_make_resource(env, @ptrCast(@alignCast(ft)));
    erl.enif_release_resource(@ptrCast(@alignCast(ft)));
    return term;
}

fn transactionSet(env: ?*erl.ErlNifEnv, argc: c_int, argv: [*c]const erl.ERL_NIF_TERM) callconv(.c) erl.ERL_NIF_TERM {
    var transaction: *Transaction = undefined;
    var key_name: ?*u8 = null;
    var key_name_length: c_int = 0;
    var value: ?*u8 = null;
    var value_length: c_int = 0;
    if (argc != 3) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_resource(env, argv[0], Resources.TRANSACTION, @ptrCast(&transaction)) == 0) {
        return erl.enif_make_badarg(env);
    }
    var key_binary: erl.ErlNifBinary = undefined;
    if (erl.enif_inspect_binary(env, argv[1], &key_binary) == 0) {
        return erl.enif_make_badarg(env);
    }
    key_name = key_binary.data;
    key_name_length = @intCast(key_binary.size);
    var value_binary: erl.ErlNifBinary = undefined;
    if (erl.enif_inspect_binary(env, argv[2], &value_binary) == 0) {
        return erl.enif_make_badarg(env);
    }
    value = value_binary.data;
    value_length = @intCast(value_binary.size);
    fdb.fdb_transaction_set(transaction.handle, key_name, key_name_length, value, value_length);
    return Atoms.OK;
}

fn transactionClear(env: ?*erl.ErlNifEnv, argc: c_int, argv: [*c]const erl.ERL_NIF_TERM) callconv(.c) erl.ERL_NIF_TERM {
    var transaction: *Transaction = undefined;
    var key_name: ?*u8 = null;
    var key_name_length: c_int = 0;
    if (argc != 2) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_resource(env, argv[0], Resources.TRANSACTION, @ptrCast(&transaction)) == 0) {
        return erl.enif_make_badarg(env);
    }
    var key_binary: erl.ErlNifBinary = undefined;
    if (erl.enif_inspect_binary(env, argv[1], &key_binary) == 0) {
        return erl.enif_make_badarg(env);
    }
    key_name = key_binary.data;
    key_name_length = @intCast(key_binary.size);
    fdb.fdb_transaction_clear(transaction.handle, key_name, key_name_length);
    return Atoms.OK;
}

fn transactionClearRange(env: ?*erl.ErlNifEnv, argc: c_int, argv: [*c]const erl.ERL_NIF_TERM) callconv(.c) erl.ERL_NIF_TERM {
    var transaction: *Transaction = undefined;
    var begin_key_name: ?*u8 = null;
    var begin_key_name_length: c_int = 0;
    var end_key_name: ?*u8 = null;
    var end_key_name_length: c_int = 0;
    if (argc != 3) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_resource(env, argv[0], Resources.TRANSACTION, @ptrCast(&transaction)) == 0) {
        return erl.enif_make_badarg(env);
    }
    var begin_key_binary: erl.ErlNifBinary = undefined;
    if (erl.enif_inspect_binary(env, argv[1], &begin_key_binary) == 0) {
        return erl.enif_make_badarg(env);
    }
    begin_key_name = begin_key_binary.data;
    begin_key_name_length = @intCast(begin_key_binary.size);
    var end_key_binary: erl.ErlNifBinary = undefined;
    if (erl.enif_inspect_binary(env, argv[2], &end_key_binary) == 0) {
        return erl.enif_make_badarg(env);
    }
    end_key_name = end_key_binary.data;
    end_key_name_length = @intCast(end_key_binary.size);
    fdb.fdb_transaction_clear_range(transaction.handle, begin_key_name, begin_key_name_length, end_key_name, end_key_name_length);
    return Atoms.OK;
}

fn transactionAtomicOp(env: ?*erl.ErlNifEnv, argc: c_int, argv: [*c]const erl.ERL_NIF_TERM) callconv(.c) erl.ERL_NIF_TERM {
    var transaction: *Transaction = undefined;
    var key_name: ?*u8 = null;
    var key_name_length: c_int = 0;
    var param: ?*u8 = null;
    var param_length: c_int = 0;
    var operation_type: fdb.FDBMutationType = undefined;
    if (argc != 4) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_resource(env, argv[0], Resources.TRANSACTION, @ptrCast(&transaction)) == 0) {
        return erl.enif_make_badarg(env);
    }
    var key_binary: erl.ErlNifBinary = undefined;
    if (erl.enif_inspect_binary(env, argv[1], &key_binary) == 0) {
        return erl.enif_make_badarg(env);
    }
    key_name = key_binary.data;
    key_name_length = @intCast(key_binary.size);
    var param_binary: erl.ErlNifBinary = undefined;
    if (erl.enif_inspect_binary(env, argv[2], &param_binary) == 0) {
        return erl.enif_make_badarg(env);
    }
    param = param_binary.data;
    param_length = @intCast(param_binary.size);
    if (erl.enif_get_uint(env, argv[3], &operation_type) == 0) {
        return erl.enif_make_badarg(env);
    }
    fdb.fdb_transaction_atomic_op(transaction.handle, key_name, key_name_length, param, param_length, operation_type);
    return Atoms.OK;
}

fn transactionCommit(env: ?*erl.ErlNifEnv, argc: c_int, argv: [*c]const erl.ERL_NIF_TERM) callconv(.c) erl.ERL_NIF_TERM {
    var transaction: *Transaction = undefined;
    if (argc != 1) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_resource(env, argv[0], Resources.TRANSACTION, @ptrCast(&transaction)) == 0) {
        return erl.enif_make_badarg(env);
    }
    const future: ?*fdb.FDBFuture = fdb.fdb_transaction_commit(transaction.handle);
    const ft: *Future = @ptrCast(@alignCast(erl.enif_alloc_resource(Resources.FUTURE, @sizeOf(Future))));
    ft.handle = future.?;
    ft.resource = @ptrCast(transaction);
    ft.type = .void;
    erl.enif_keep_resource(ft.resource);
    const term: erl.ERL_NIF_TERM = erl.enif_make_resource(env, @ptrCast(@alignCast(ft)));
    erl.enif_release_resource(@ptrCast(@alignCast(ft)));
    return term;
}

fn transactionGetCommittedVersion(env: ?*erl.ErlNifEnv, argc: c_int, argv: [*c]const erl.ERL_NIF_TERM) callconv(.c) erl.ERL_NIF_TERM {
    var transaction: *Transaction = undefined;
    var version: i64 = undefined;
    if (argc != 1) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_resource(env, argv[0], Resources.TRANSACTION, @ptrCast(&transaction)) == 0) {
        return erl.enif_make_badarg(env);
    }
    const err: c_int = fdb.fdb_transaction_get_committed_version(transaction.handle, &version);
    if (err != 0) {
        return handleError(env, err);
    }
    return erl.enif_make_tuple2(env, Atoms.OK, erl.enif_make_int64(env, version));
}

fn transactionGetTagThrottledDuration(env: ?*erl.ErlNifEnv, argc: c_int, argv: [*c]const erl.ERL_NIF_TERM) callconv(.c) erl.ERL_NIF_TERM {
    var transaction: *Transaction = undefined;
    if (argc != 1) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_resource(env, argv[0], Resources.TRANSACTION, @ptrCast(&transaction)) == 0) {
        return erl.enif_make_badarg(env);
    }
    const future: ?*fdb.FDBFuture = fdb.fdb_transaction_get_tag_throttled_duration(transaction.handle);
    const ft: *Future = @ptrCast(@alignCast(erl.enif_alloc_resource(Resources.FUTURE, @sizeOf(Future))));
    ft.handle = future.?;
    ft.resource = @ptrCast(transaction);
    ft.type = .double;
    erl.enif_keep_resource(ft.resource);
    const term: erl.ERL_NIF_TERM = erl.enif_make_resource(env, @ptrCast(@alignCast(ft)));
    erl.enif_release_resource(@ptrCast(@alignCast(ft)));
    return term;
}

fn transactionGetTotalCost(env: ?*erl.ErlNifEnv, argc: c_int, argv: [*c]const erl.ERL_NIF_TERM) callconv(.c) erl.ERL_NIF_TERM {
    var transaction: *Transaction = undefined;
    if (argc != 1) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_resource(env, argv[0], Resources.TRANSACTION, @ptrCast(&transaction)) == 0) {
        return erl.enif_make_badarg(env);
    }
    const future: ?*fdb.FDBFuture = fdb.fdb_transaction_get_total_cost(transaction.handle);
    const ft: *Future = @ptrCast(@alignCast(erl.enif_alloc_resource(Resources.FUTURE, @sizeOf(Future))));
    ft.handle = future.?;
    ft.resource = @ptrCast(transaction);
    ft.type = .int64;
    erl.enif_keep_resource(ft.resource);
    const term: erl.ERL_NIF_TERM = erl.enif_make_resource(env, @ptrCast(@alignCast(ft)));
    erl.enif_release_resource(@ptrCast(@alignCast(ft)));
    return term;
}

fn transactionGetApproximateSize(env: ?*erl.ErlNifEnv, argc: c_int, argv: [*c]const erl.ERL_NIF_TERM) callconv(.c) erl.ERL_NIF_TERM {
    var transaction: *Transaction = undefined;
    if (argc != 1) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_resource(env, argv[0], Resources.TRANSACTION, @ptrCast(&transaction)) == 0) {
        return erl.enif_make_badarg(env);
    }
    const future: ?*fdb.FDBFuture = fdb.fdb_transaction_get_approximate_size(transaction.handle);
    const ft: *Future = @ptrCast(@alignCast(erl.enif_alloc_resource(Resources.FUTURE, @sizeOf(Future))));
    ft.handle = future.?;
    ft.resource = @ptrCast(transaction);
    ft.type = .int64;
    erl.enif_keep_resource(ft.resource);
    const term: erl.ERL_NIF_TERM = erl.enif_make_resource(env, @ptrCast(@alignCast(ft)));
    erl.enif_release_resource(@ptrCast(@alignCast(ft)));
    return term;
}

fn transactionGetVersionstamp(env: ?*erl.ErlNifEnv, argc: c_int, argv: [*c]const erl.ERL_NIF_TERM) callconv(.c) erl.ERL_NIF_TERM {
    var transaction: *Transaction = undefined;
    if (argc != 1) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_resource(env, argv[0], Resources.TRANSACTION, @ptrCast(&transaction)) == 0) {
        return erl.enif_make_badarg(env);
    }
    const future: ?*fdb.FDBFuture = fdb.fdb_transaction_get_versionstamp(transaction.handle);
    const ft: *Future = @ptrCast(@alignCast(erl.enif_alloc_resource(Resources.FUTURE, @sizeOf(Future))));
    ft.handle = future.?;
    ft.resource = @ptrCast(transaction);
    ft.type = .key;
    erl.enif_keep_resource(ft.resource);
    const term: erl.ERL_NIF_TERM = erl.enif_make_resource(env, @ptrCast(@alignCast(ft)));
    erl.enif_release_resource(@ptrCast(@alignCast(ft)));
    return term;
}

fn transactionWatch(env: ?*erl.ErlNifEnv, argc: c_int, argv: [*c]const erl.ERL_NIF_TERM) callconv(.c) erl.ERL_NIF_TERM {
    var transaction: *Transaction = undefined;
    var key_name: ?*u8 = null;
    var key_name_length: c_int = 0;
    if (argc != 2) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_resource(env, argv[0], Resources.TRANSACTION, @ptrCast(&transaction)) == 0) {
        return erl.enif_make_badarg(env);
    }
    var key_binary: erl.ErlNifBinary = undefined;
    if (erl.enif_inspect_binary(env, argv[1], &key_binary) == 0) {
        return erl.enif_make_badarg(env);
    }
    key_name = key_binary.data;
    key_name_length = @intCast(key_binary.size);
    const future: ?*fdb.FDBFuture = fdb.fdb_transaction_watch(transaction.handle, key_name, key_name_length);
    const ft: *Future = @ptrCast(@alignCast(erl.enif_alloc_resource(Resources.FUTURE, @sizeOf(Future))));
    ft.handle = future.?;
    ft.resource = null; // Watchers outlive their transactions
    ft.type = .void;
    const term: erl.ERL_NIF_TERM = erl.enif_make_resource(env, @ptrCast(@alignCast(ft)));
    erl.enif_release_resource(@ptrCast(@alignCast(ft)));
    return term;
}

fn transactionOnError(env: ?*erl.ErlNifEnv, argc: c_int, argv: [*c]const erl.ERL_NIF_TERM) callconv(.c) erl.ERL_NIF_TERM {
    var transaction: *Transaction = undefined;
    var err: c_int = undefined;
    if (argc != 2) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_resource(env, argv[0], Resources.TRANSACTION, @ptrCast(&transaction)) == 0) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_int(env, argv[1], &err) == 0) {
        return erl.enif_make_badarg(env);
    }
    const future: ?*fdb.FDBFuture = fdb.fdb_transaction_on_error(transaction.handle, err);
    const ft: *Future = @ptrCast(@alignCast(erl.enif_alloc_resource(Resources.FUTURE, @sizeOf(Future))));
    ft.handle = future.?;
    ft.resource = @ptrCast(transaction);
    ft.type = .void;
    erl.enif_keep_resource(ft.resource);
    const term: erl.ERL_NIF_TERM = erl.enif_make_resource(env, @ptrCast(@alignCast(ft)));
    erl.enif_release_resource(@ptrCast(@alignCast(ft)));
    return term;
}

fn transactionReset(env: ?*erl.ErlNifEnv, argc: c_int, argv: [*c]const erl.ERL_NIF_TERM) callconv(.c) erl.ERL_NIF_TERM {
    var transaction: *Transaction = undefined;
    if (argc != 1) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_resource(env, argv[0], Resources.TRANSACTION, @ptrCast(&transaction)) == 0) {
        return erl.enif_make_badarg(env);
    }
    fdb.fdb_transaction_reset(transaction.handle);
    return Atoms.OK;
}

fn transactionCancel(env: ?*erl.ErlNifEnv, argc: c_int, argv: [*c]const erl.ERL_NIF_TERM) callconv(.c) erl.ERL_NIF_TERM {
    var transaction: *Transaction = undefined;
    if (argc != 1) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_resource(env, argv[0], Resources.TRANSACTION, @ptrCast(&transaction)) == 0) {
        return erl.enif_make_badarg(env);
    }
    fdb.fdb_transaction_cancel(transaction.handle);
    return Atoms.OK;
}

fn transactionAddConflictRange(env: ?*erl.ErlNifEnv, argc: c_int, argv: [*c]const erl.ERL_NIF_TERM) callconv(.c) erl.ERL_NIF_TERM {
    var transaction: *Transaction = undefined;
    var begin_key_name: ?*u8 = null;
    var begin_key_name_length: c_int = 0;
    var end_key_name: ?*u8 = null;
    var end_key_name_length: c_int = 0;
    var ty: fdb.FDBConflictRangeType = undefined;
    if (argc != 4) {
        return erl.enif_make_badarg(env);
    }
    if (erl.enif_get_resource(env, argv[0], Resources.TRANSACTION, @ptrCast(&transaction)) == 0) {
        return erl.enif_make_badarg(env);
    }
    var begin_key_binary: erl.ErlNifBinary = undefined;
    if (erl.enif_inspect_binary(env, argv[1], &begin_key_binary) == 0) {
        return erl.enif_make_badarg(env);
    }
    begin_key_name = begin_key_binary.data;
    begin_key_name_length = @intCast(begin_key_binary.size);
    var end_key_binary: erl.ErlNifBinary = undefined;
    if (erl.enif_inspect_binary(env, argv[2], &end_key_binary) == 0) {
        return erl.enif_make_badarg(env);
    }
    end_key_name = end_key_binary.data;
    end_key_name_length = @intCast(end_key_binary.size);
    if (erl.enif_get_uint(env, argv[3], &ty) == 0) {
        return erl.enif_make_badarg(env);
    }
    const err: c_int = fdb.fdb_transaction_add_conflict_range(transaction.handle, begin_key_name, begin_key_name_length, end_key_name, end_key_name_length, ty);
    return handleResult(env, err);
}

const Atoms = struct {
    var ENOMEM: erl.ERL_NIF_TERM = undefined;
    var ERROR: erl.ERL_NIF_TERM = undefined;
    var FUTURE: erl.ERL_NIF_TERM = undefined;
    var NIL: erl.ERL_NIF_TERM = undefined;
    var OK: erl.ERL_NIF_TERM = undefined;
};

const Resources = struct {
    var FDB_DATABASE: ?*erl.ErlNifResourceType = null;
    var FUTURE: ?*erl.ErlNifResourceType = null;
    var FDB_TENANT: ?*erl.ErlNifResourceType = null;
    var TRANSACTION: ?*erl.ErlNifResourceType = null;
};

fn database_destructor(_: ?*erl.ErlNifEnv, res: ?*anyopaque) callconv(.c) void {
    const db: **fdb.FDBDatabase = @ptrCast(@alignCast(res));
    fdb.fdb_database_destroy(db.*);
}

fn future_destructor(_: ?*erl.ErlNifEnv, res: ?*anyopaque) callconv(.c) void {
    const future: *Future = @ptrCast(@alignCast(res));
    fdb.fdb_future_destroy(future.handle);
    if (future.resource != null) {
        erl.enif_release_resource(future.resource);
    }
}

fn tenant_destructor(_: ?*erl.ErlNifEnv, res: ?*anyopaque) callconv(.c) void {
    const tenant: **fdb.FDBTenant = @ptrCast(@alignCast(res));
    fdb.fdb_tenant_destroy(tenant.*);
}

fn transaction_destructor(_: ?*erl.ErlNifEnv, res: ?*anyopaque) callconv(.c) void {
    const transaction: *Transaction = @ptrCast(@alignCast(res));
    fdb.fdb_transaction_destroy(transaction.handle);
    if (transaction.resource != null) {
        erl.enif_release_resource(transaction.resource);
    }
}

fn load(env: ?*erl.ErlNifEnv, _: [*c]?*anyopaque, _: erl.ERL_NIF_TERM) callconv(.c) c_int {
    Atoms.ENOMEM = erl.enif_make_atom(env, "enomem");
    Atoms.ERROR = erl.enif_make_atom(env, "error");
    Atoms.NIL = erl.enif_make_atom(env, "nil");
    Atoms.OK = erl.enif_make_atom(env, "ok");

    const flags: c_int = erl.ERL_NIF_RT_CREATE | erl.ERL_NIF_RT_TAKEOVER;
    Resources.FDB_DATABASE =
        erl.enif_open_resource_type(env, null, "database", database_destructor, flags, null);
    Resources.FUTURE =
        erl.enif_open_resource_type(env, null, "future", future_destructor, flags, null);
    Resources.FDB_TENANT =
        erl.enif_open_resource_type(env, null, "tenant", tenant_destructor, flags, null);
    Resources.TRANSACTION =
        erl.enif_open_resource_type(env, null, "transaction", transaction_destructor, flags, null);

    return 0;
}

var funcs = [_]erl.ErlNifFunc{
    erl.ErlNifFunc{
        .name = "select_api_version",
        .arity = 1,
        .fptr = selectApiVersion,
        .flags = 0,
    },
    erl.ErlNifFunc{
        .name = "get_max_api_version",
        .arity = 0,
        .fptr = getMaxApiVersion,
        .flags = 0,
    },
    erl.ErlNifFunc{
        .name = "network_set_option",
        .arity = 1,
        .fptr = networkSetOption,
        .flags = 0,
    },
    erl.ErlNifFunc{
        .name = "network_set_option",
        .arity = 2,
        .fptr = networkSetOption,
        .flags = 0,
    },
    erl.ErlNifFunc{
        .name = "setup_network",
        .arity = 0,
        .fptr = setupNetwork,
        .flags = 0,
    },
    erl.ErlNifFunc{
        .name = "run_network",
        .arity = 0,
        .fptr = runNetwork,
        .flags = 0,
    },
    erl.ErlNifFunc{
        .name = "stop_network",
        .arity = 0,
        .fptr = stopNetwork,
        .flags = 0,
    },
    erl.ErlNifFunc{
        .name = "future_resolve",
        .arity = 2,
        .fptr = futureResolve,
        .flags = 0,
    },
    erl.ErlNifFunc{
        .name = "future_cancel",
        .arity = 1,
        .fptr = futureCancel,
        .flags = 0,
    },
    erl.ErlNifFunc{
        .name = "future_is_ready",
        .arity = 1,
        .fptr = futureIsReady,
        .flags = 0,
    },
    erl.ErlNifFunc{
        .name = "create_database",
        .arity = 1,
        .fptr = createDatabase,
        .flags = 0,
    },
    erl.ErlNifFunc{
        .name = "database_set_option",
        .arity = 2,
        .fptr = databaseSetOption,
        .flags = 0,
    },
    erl.ErlNifFunc{
        .name = "database_set_option",
        .arity = 3,
        .fptr = databaseSetOption,
        .flags = 0,
    },
    erl.ErlNifFunc{
        .name = "database_open_tenant",
        .arity = 2,
        .fptr = databaseOpenTenant,
        .flags = 0,
    },
    erl.ErlNifFunc{
        .name = "database_create_transaction",
        .arity = 1,
        .fptr = databaseCreateTransaction,
        .flags = 0,
    },
    erl.ErlNifFunc{
        .name = "database_reboot_worker",
        .arity = 4,
        .fptr = databaseRebootWorker,
        .flags = 0,
    },
    erl.ErlNifFunc{
        .name = "database_force_recovery_with_data_loss",
        .arity = 2,
        .fptr = databaseForceRecoveryWithDataLoss,
        .flags = 0,
    },
    erl.ErlNifFunc{
        .name = "database_create_snapshot",
        .arity = 2,
        .fptr = databaseCreateSnapshot,
        .flags = 0,
    },
    erl.ErlNifFunc{
        .name = "database_get_main_thread_busyness",
        .arity = 1,
        .fptr = databaseGetMainThreadBusyness,
        .flags = 0,
    },
    erl.ErlNifFunc{
        .name = "database_get_client_status",
        .arity = 1,
        .fptr = databaseGetClientStatus,
        .flags = 0,
    },
    erl.ErlNifFunc{
        .name = "tenant_create_transaction",
        .arity = 1,
        .fptr = tenantCreateTransaction,
        .flags = 0,
    },
    erl.ErlNifFunc{
        .name = "tenant_get_id",
        .arity = 1,
        .fptr = tenantGetId,
        .flags = 0,
    },
    erl.ErlNifFunc{
        .name = "transaction_set_option",
        .arity = 2,
        .fptr = transactionSetOption,
        .flags = 0,
    },
    erl.ErlNifFunc{
        .name = "transaction_set_option",
        .arity = 3,
        .fptr = transactionSetOption,
        .flags = 0,
    },
    erl.ErlNifFunc{
        .name = "transaction_set_read_version",
        .arity = 2,
        .fptr = transactionSetReadVersion,
        .flags = 0,
    },
    erl.ErlNifFunc{
        .name = "transaction_get_read_version",
        .arity = 1,
        .fptr = transactionGetReadVersion,
        .flags = 0,
    },
    erl.ErlNifFunc{
        .name = "transaction_get",
        .arity = 3,
        .fptr = transactionGet,
        .flags = 0,
    },
    erl.ErlNifFunc{
        .name = "transaction_get_estimated_range_size_bytes",
        .arity = 3,
        .fptr = transactionGetEstimatedRangeSizeBytes,
        .flags = 0,
    },
    erl.ErlNifFunc{
        .name = "transaction_get_range_split_points",
        .arity = 4,
        .fptr = transactionGetRangeSplitPoints,
        .flags = 0,
    },
    erl.ErlNifFunc{
        .name = "transaction_get_key",
        .arity = 5,
        .fptr = transactionGetKey,
        .flags = 0,
    },
    erl.ErlNifFunc{
        .name = "transaction_get_addresses_for_key",
        .arity = 2,
        .fptr = transactionGetAddressesForKey,
        .flags = 0,
    },
    erl.ErlNifFunc{
        .name = "transaction_get_range",
        .arity = 13,
        .fptr = transactionGetRange,
        .flags = 0,
    },
    erl.ErlNifFunc{
        .name = "transaction_set",
        .arity = 3,
        .fptr = transactionSet,
        .flags = 0,
    },
    erl.ErlNifFunc{
        .name = "transaction_clear",
        .arity = 2,
        .fptr = transactionClear,
        .flags = 0,
    },
    erl.ErlNifFunc{
        .name = "transaction_clear_range",
        .arity = 3,
        .fptr = transactionClearRange,
        .flags = 0,
    },
    erl.ErlNifFunc{
        .name = "transaction_atomic_op",
        .arity = 4,
        .fptr = transactionAtomicOp,
        .flags = 0,
    },
    erl.ErlNifFunc{
        .name = "transaction_commit",
        .arity = 1,
        .fptr = transactionCommit,
        .flags = 0,
    },
    erl.ErlNifFunc{
        .name = "transaction_get_committed_version",
        .arity = 1,
        .fptr = transactionGetCommittedVersion,
        .flags = 0,
    },
    erl.ErlNifFunc{
        .name = "transaction_get_tag_throttled_duration",
        .arity = 1,
        .fptr = transactionGetTagThrottledDuration,
        .flags = 0,
    },
    erl.ErlNifFunc{
        .name = "transaction_get_total_cost",
        .arity = 1,
        .fptr = transactionGetTotalCost,
        .flags = 0,
    },
    erl.ErlNifFunc{
        .name = "transaction_get_approximate_size",
        .arity = 1,
        .fptr = transactionGetApproximateSize,
        .flags = 0,
    },
    erl.ErlNifFunc{
        .name = "transaction_get_versionstamp",
        .arity = 1,
        .fptr = transactionGetVersionstamp,
        .flags = 0,
    },
    erl.ErlNifFunc{
        .name = "transaction_watch",
        .arity = 2,
        .fptr = transactionWatch,
        .flags = 0,
    },
    erl.ErlNifFunc{
        .name = "transaction_on_error",
        .arity = 2,
        .fptr = transactionOnError,
        .flags = 0,
    },
    erl.ErlNifFunc{
        .name = "transaction_reset",
        .arity = 1,
        .fptr = transactionReset,
        .flags = 0,
    },
    erl.ErlNifFunc{
        .name = "transaction_cancel",
        .arity = 1,
        .fptr = transactionCancel,
        .flags = 0,
    },
    erl.ErlNifFunc{
        .name = "transaction_add_conflict_range",
        .arity = 4,
        .fptr = transactionAddConflictRange,
        .flags = 0,
    },
};

var entry = erl.ErlNifEntry{
    .major = erl.ERL_NIF_MAJOR_VERSION,
    .minor = erl.ERL_NIF_MINOR_VERSION,
    .name = "Elixir.FDBC.NIF",
    .num_of_funcs = funcs.len,
    .funcs = &funcs,
    .load = load,
    .reload = null,
    .upgrade = null,
    .unload = null,
    .vm_variant = erl.ERL_NIF_VM_VARIANT,
    .options = 1,
    .sizeof_ErlNifResourceTypeInit = @sizeOf(erl.ErlNifResourceTypeInit),
    .min_erts = erl.ERL_NIF_MIN_ERTS_VERSION,
};

export fn nif_init() *erl.ErlNifEntry {
    return &entry;
}
