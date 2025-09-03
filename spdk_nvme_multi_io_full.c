/*
指定 CPU cores
創建多個 reactor thread （每個cpu core各一)
為每個 reactor thread: 建立多個 IO channel (io channel: 即 NVMe queue pair)
做簡單讀寫操作
*/
#include "spdk/stdinc.h"
#include "spdk/env.h"
#include "spdk/nvme.h"
#include "spdk/nvme_intel.h"

#define NUM_REACTORS 2     // Reactor thread 數量
#define QP_PER_REACTOR 1   // 每個 reactor 的 queue pair 數
#define TEST_IO_SIZE 4096  // 每次 IO 大小

struct reactor_context {
    struct spdk_nvme_ctrlr *ctrlr;
    struct spdk_nvme_qpair *qpair[QP_PER_REACTOR];
    uint64_t io_submitted;
    uint64_t io_completed;
};

static void io_complete(void *arg, const struct spdk_nvme_cpl *cpl)
{
    struct reactor_context *ctx = arg;
    ctx->io_completed++;
}

static void submit_io(struct reactor_context *ctx)
{
    void *buf = spdk_zmalloc(TEST_IO_SIZE, 0x1000, NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
    if (!buf) {
        fprintf(stderr, "Failed to allocate buffer\n");
        return;
    }

    for (int q = 0; q < QP_PER_REACTOR; q++) {
        struct spdk_nvme_qpair *qp = ctx->qpair[q];
        uint64_t lba = 0;

        int rc = spdk_nvme_ns_cmd_read(spdk_nvme_ctrlr_get_ns(ctx->ctrlr, 1),
                                       qp, buf, lba, 1, io_complete, ctx, 0);
        if (rc == 0) {
            ctx->io_submitted++;
        } else {
            fprintf(stderr, "Failed to submit IO\n");
        }
    }
}

static int reactor_thread(void *arg)
{
    struct reactor_context *ctx = arg;
    int core = spdk_env_get_current_core();

    printf("Reactor thread started on core %d\n", core);

    // 創建每個 reactor 的 queue pair
    for (int i = 0; i < QP_PER_REACTOR; i++) {
        ctx->qpair[i] = spdk_nvme_ctrlr_alloc_io_qpair(ctx->ctrlr, NULL, 0);
        if (!ctx->qpair[i]) {
            fprintf(stderr, "Failed to allocate qpair for core %d\n", core);
            return -1;
        }
    }

    // 發送初始 IO
    submit_io(ctx);

    // Reactor loop: 處理完成事件
    while (true) {
        for (int i = 0; i < QP_PER_REACTOR; i++) {
            spdk_nvme_qpair_process_completions(ctx->qpair[i], 0);
        }
        usleep(1000); // 簡單 poll
    }

    return 0;
}

int main(int argc, char **argv)
{
    struct spdk_env_opts opts;
    struct reactor_context ctx[NUM_REACTORS];

    spdk_env_opts_init(&opts);
    opts.name = "spdk_multi_core_example";
    opts.core_mask = "0x3"; // Core 0 和 Core 1
    if (spdk_env_init(&opts) < 0) {
        fprintf(stderr, "Unable to initialize SPDK env\n");
        return -1;
    }

    // 探測 NVMe 控制器
    ctx[0].ctrlr = spdk_nvme_connect(NULL, 0, 0, NULL);
    if (!ctx[0].ctrlr) {
        fprintf(stderr, "Failed to connect NVMe controller\n");
        return -1;
    }

    // 所有 reactor 使用同一個 ctrlr
    for (int i = 1; i < NUM_REACTORS; i++) {
        ctx[i].ctrlr = ctx[0].ctrlr;
        ctx[i].io_submitted = 0;
        ctx[i].io_completed = 0;
    }

    // 啟動 reactor threads
    for (int i = 0; i < NUM_REACTORS; i++) {
        spdk_env_thread_launch_pinned(i, reactor_thread, &ctx[i]);
    }

    // 主 thread 也可以監控 IO 完成數
    while (true) {
        for (int i = 0; i < NUM_REACTORS; i++) {
            printf("Core %d: submitted=%lu completed=%lu\n",
                   i, ctx[i].io_submitted, ctx[i].io_completed);
        }
        sleep(1);
    }

    return 0;
}

