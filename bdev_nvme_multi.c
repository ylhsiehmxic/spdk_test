/*
多核 Reactor → I/O Channel → Queue → BDEV IO
每個 core 發送多個 I/O
簡單統計吞吐量
*/
#include "spdk/stdinc.h"
#include "spdk/env.h"
#include "spdk/bdev.h"
#include "spdk/bdev_module.h"
#include "spdk/io_channel.h"

#define NUM_REACTORS 2       // 使用的 reactor 數
#define IO_PER_REACTOR 4     // 每個 reactor 同時發送 IO 數
#define TEST_IO_SIZE 4096    // 每次 IO 大小

struct reactor_ctx {
    struct spdk_bdev *bdev;
    struct spdk_bdev_desc *desc;
    struct spdk_io_channel *ch;
    uint64_t io_submitted;
    uint64_t io_completed;
};

// I/O 完成回調
static void io_complete(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
    struct reactor_ctx *ctx = cb_arg;
    ctx->io_completed++;

    if (!success) {
        fprintf(stderr, "I/O failed\n");
    }

    spdk_bdev_free_io(bdev_io);
}

// 提交多個 IO
static void submit_io(struct reactor_ctx *ctx)
{
    for (int i = 0; i < IO_PER_REACTOR; i++) {
        void *buf = spdk_zmalloc(TEST_IO_SIZE, 0x1000, NULL,
                                 SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
        if (!buf) {
            fprintf(stderr, "Failed to allocate buffer\n");
            continue;
        }

        int rc = spdk_bdev_read(ctx->desc, ctx->ch, buf, i * TEST_IO_SIZE,
                                TEST_IO_SIZE, io_complete, ctx);
        if (rc == 0) {
            ctx->io_submitted++;
        } else {
            fprintf(stderr, "Failed to submit bdev read\n");
            spdk_free(buf);
        }
    }
}

// Reactor thread function
static int reactor_thread(void *arg)
{
    struct reactor_ctx *ctx = arg;
    int core = spdk_env_get_current_core();
    printf("Reactor thread started on core %d\n", core);

    // 每個 reactor 建立 I/O channel
    ctx->ch = spdk_bdev_get_io_channel(ctx->desc);
    if (!ctx->ch) {
        fprintf(stderr, "Failed to get IO channel for core %d\n", core);
        return -1;
    }

    submit_io(ctx);

    // Reactor poll loop
    while (true) {
        spdk_bdev_poll(ctx->ch);
        usleep(1000);
    }

    return 0;
}

int main(int argc, char **argv)
{
    struct spdk_env_opts opts;
    struct reactor_ctx ctx[NUM_REACTORS];

    spdk_env_opts_init(&opts);
    opts.name = "spdk_bdev_multi_core";
    opts.core_mask = "0x3"; // Core 0,1
    if (spdk_env_init(&opts) < 0) {
        fprintf(stderr, "SPDK env init failed\n");
        return -1;
    }

    // 取得 NVMe BDEV
    struct spdk_bdev *bdev = spdk_bdev_get_by_name("Nvme0n1");
    if (!bdev) {
        fprintf(stderr, "NVMe BDEV not found\n");
        return -1;
    }

    // 打開 BDEV
    if (spdk_bdev_open(bdev, true, NULL, NULL, &ctx[0].desc) != 0) {
        fprintf(stderr, "Failed to open BDEV\n");
        return -1;
    }

    // 每個 reactor 共用同一個 desc
    for (int i = 0; i < NUM_REACTORS; i++) {
        ctx[i].bdev = bdev;
        ctx[i].desc = ctx[0].desc;
        ctx[i].io_submitted = 0;
        ctx[i].io_completed = 0;
    }

    // 啟動 reactor threads
    for (int i = 0; i < NUM_REACTORS; i++) {
        spdk_env_thread_launch_pinned(i, reactor_thread, &ctx[i]);
    }

    // 主 thread 監控 IO 完成
    while (true) {
        for (int i = 0; i < NUM_REACTORS; i++) {
            printf("Core %d: submitted=%lu completed=%lu\n",
                   i, ctx[i].io_submitted, ctx[i].io_completed);
        }
        sleep(1);
    }

    // 清理資源 (示範，程式目前會 infinite loop)
    // for (int i = 0; i < NUM_REACTORS; i++) {
    //     spdk_put_io_channel(ctx[i].ch);
    // }
    // spdk_bdev_close(ctx[0].desc);

    return 0;
}

