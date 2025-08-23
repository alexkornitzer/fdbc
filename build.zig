const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const erts_include = b.run(&.{ "erl", "-noshell", "-eval", "io:format(\"~s\", [filename:join([lists:concat([code:root_dir(), \"/erts-\", erlang:system_info(version)]), \"include\"])]), init:stop(0)." });

    const libnif = b.addLibrary(.{
        .linkage = .dynamic,
        .name = "fdbcnif",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/fdbc.zig"),
            .target = target,
            .optimize = optimize,
        }),
        .version = null,
    });
    libnif.addIncludePath(.{ .cwd_relative = erts_include });
    libnif.linker_allow_shlib_undefined = true;

    libnif.addIncludePath(.{ .cwd_relative = "/usr/local/include/" });
    libnif.addLibraryPath(.{ .cwd_relative = "/usr/local/lib/" });

    libnif.linkSystemLibrary("fdb_c");

    b.installArtifact(libnif);
}
