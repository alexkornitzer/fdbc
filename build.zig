const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const erts_include = b.run(&.{ "erl", "-noshell", "-eval", "io:format(\"~s\", [filename:join([lists:concat([code:root_dir(), \"/erts-\", erlang:system_info(version)]), \"include\"])]), init:stop(0)." });

    const translate_erl = b.addTranslateC(.{
        .root_source_file = b.path("src/erl.h"),
        .target = target,
        .optimize = optimize,
    });

    translate_erl.addIncludePath(.{ .cwd_relative = erts_include });

    const libnif = b.addLibrary(.{
        .linkage = .dynamic,
        .name = "fdbcnif",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/fdbc.zig"),
            .optimize = optimize,
            .target = target,
            .imports = &.{
                .{
                    .name = "erl",
                    .module = translate_erl.createModule(),
                },
            },
        }),
        .version = null,
    });

    libnif.linker_allow_shlib_undefined = true;
    libnif.root_module.addIncludePath(.{ .cwd_relative = "/usr/local/include/" });
    libnif.root_module.addLibraryPath(.{ .cwd_relative = "/usr/local/lib/" });
    libnif.root_module.linkSystemLibrary("fdb_c", .{});

    b.installArtifact(libnif);
}
