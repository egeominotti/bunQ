const std = @import("std");

const Job = struct {
    id: u64,
    priority: i32,
};

const Queue = struct {
    items: std.ArrayList(Job),
    allocator: std.mem.Allocator,

    fn init(allocator: std.mem.Allocator) Queue {
        return .{
            .items = std.ArrayList(Job).init(allocator),
            .allocator = allocator,
        };
    }

    fn deinit(self: *Queue) void {
        self.items.deinit();
    }

    fn push(self: *Queue, id: u64, priority: i32) bool {
        // Find insertion point (higher priority first)
        var i: usize = 0;
        while (i < self.items.items.len) : (i += 1) {
            if (self.items.items[i].priority < priority) break;
        }
        self.items.insert(i, .{ .id = id, .priority = priority }) catch return false;
        return true;
    }

    fn pull(self: *Queue) ?u64 {
        if (self.items.items.len == 0) return null;
        const job = self.items.orderedRemove(0);
        return job.id;
    }

    fn len(self: *Queue) usize {
        return self.items.items.len;
    }
};

// Global allocator for FFI
var gpa = std.heap.GeneralPurposeAllocator(.{}){};

// ============ Exported C API ============

export fn queue_create() ?*Queue {
    const allocator = gpa.allocator();
    const queue = allocator.create(Queue) catch return null;
    queue.* = Queue.init(allocator);
    return queue;
}

export fn queue_destroy(queue: ?*Queue) void {
    if (queue) |q| {
        q.deinit();
        gpa.allocator().destroy(q);
    }
}

export fn queue_push(queue: ?*Queue, id: u64, priority: i32) bool {
    if (queue) |q| {
        return q.push(id, priority);
    }
    return false;
}

export fn queue_pull(queue: ?*Queue) u64 {
    if (queue) |q| {
        return q.pull() orelse 0;
    }
    return 0;
}

export fn queue_len(queue: ?*Queue) u64 {
    if (queue) |q| {
        return @intCast(q.len());
    }
    return 0;
}

// Simple function to benchmark pure FFI overhead
export fn add_numbers(a: i64, b: i64) i64 {
    return a + b;
}
