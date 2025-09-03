/*
初始化 SPDK 環境
探測 NVMe BDEV（SPDK 封裝好的 NVMe block device）
創建 I/O channel
發送讀寫請求
*/
#include "spdk/stdinc.h"
#include "spdk/env.h"
#include "spdk/bdev.h"
#include "spdk/bdev_module.h"
#include "spdk/bdev_nvme.h"
#include "spdk/io_channel.h"

#define TEST_IO_SIZE 4096

struct bdev_context {
    struct spdk_bdev *bdev;
    struct spdk_bdev_desc *desc;
    struct spdk_io_channel *ch;
    uint64_t io_submitted;
    uint64_t io_completed;
};

static void io_complete(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
    struct bdev_context *ctx = cb_arg;
    ctx->io_completed++;

    if (!success) {
        fprintf(stderr, "I/O failed\n");
    }

    spdk_bdev_free_io(bdev_io);
}

static void submit_bdev_io(struct bdev_context *ctx)
{
    void *buf = spdk_malloc(TEST_IO_SIZE, 0x1000, NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
    if (!buf) {
        fprintf(stderr, "Failed to allocate buffer\n");
        return;
    }

    int rc = spdk_bdev_read(ctx->desc, ctx->ch, buf, 0, TEST_IO_SIZE, io_complete, ctx);
    if (rc == 0) {
        ctx->io_submitted++;
    } else {
        fprintf(stderr, "Failed to submit bdev read\n");
        spdk_free(buf);
    }
}

static void bdev_event_cb(enum spdk_bdev_event_type type,
                          struct spdk_bdev *bdev,
                          void *event_ctx)
{
    printf("BDEV event: type=%d, bdev=%s\n", type, spdk_bdev_get_name(bdev));
}

int main(int argc, char **argv)
{
    struct spdk_env_opts opts;
    struct bdev_context ctx;

    spdk_env_opts_init(&opts);
    opts.name = "spdk_bdev_example";
    opts.core_mask = "0x1"; // 使用 core 0
    if (spdk_env_init(&opts) < 0) {
        fprintf(stderr, "SPDK env init failed\n");
        return -1;
    }

    // 探測 NVMe BDEV
    struct spdk_bdev *bdev = spdk_bdev_get_by_name("Nvme0n1");
    if (!bdev) {
        fprintf(stderr, "NVMe bdev not found\n");
        return -1;
    }

    ctx.bdev = bdev;

    if (spdk_bdev_open(bdev, true, bdev_event_cb, NULL, &ctx.desc) != 0) {
        fprintf(stderr, "Failed to open bdev\n");
        return -1;
    }

    // 創建 I/O channel
    ctx.ch = spdk_bdev_get_io_channel(ctx.desc);
    if (!ctx.ch) {
        fprintf(stderr, "Failed to get I/O channel\n");
        return -1;
    }

    ctx.io_submitted = 0;
    ctx.io_completed = 0;

    // 提交測試 IO
    submit_bdev_io(&ctx);

    // 簡單 poll loop
    while (ctx.io_completed < ctx.io_submitted) {
        spdk_bdev_poll(ctx.ch);
        usleep(1000);
    }

    printf("Bdev IO complete: submitted=%lu completed=%lu\n", ctx.io_submitted, ctx.io_completed);

    spdk_put_io_channel(ctx.ch);
    spdk_bdev_close(ctx.desc);

    return 0;
}

