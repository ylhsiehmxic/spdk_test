/*
指定 CPU cores
創建多個 reactor thread （每個cpu core各一)
為每個 reactor thread: 建立一個 IO channel, i.e. 創建 NVMe queue pair
做簡單讀寫操作
*/
#include "spdk/stdinc.h"
#include "spdk/env.h"
#include "spdk/nvme.h"
#include "spdk/nvme_intel.h"

#define REACTOR_CORES 2  // Reactor thread 數
#define QUEUE_PER_CORE 1 // 每個 reactor 對應的 qp 數

struct app_context {
    struct spdk_nvme_ctrlr *ctrlr;
    struct spdk_nvme_qpair *qp[REACTOR_CORES];
};

static void
io_complete(void *arg, const struct spdk_nvme_cpl *cpl)
{
    printf("IO complete\n");
}

static void
submit_io(struct spdk_nvme_qpair *qp)
{
    uint64_t lba = 0;
    void *buf = spdk_malloc(4096, 0x1000, NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
    if (!buf) {
        fprintf(stderr, "Failed to allocate buffer\n");
        return;
    }

    spdk_nvme_ns_cmd_read(spdk_nvme_ctrlr_get_ns(qp->ctrlr, 1),
                          qp, buf, lba, 1, io_complete, NULL, 0);
}

static int
reactor_fn(void *arg)
{
    struct app_context *ctx = arg;

    printf("Reactor thread started on core %d\n", spdk_env_get_current_core());
    // 創建對應 core 的 qpair
    ctx->qp[spdk_env_get_current_core()] =
        spdk_nvme_ctrlr_alloc_io_qpair(ctx->ctrlr, NULL, 0);

    // 模擬發送 IO
    submit_io(ctx->qp[spdk_env_get_current_core()]);

    // Reactor loop
    while (true) {
        spdk_nvme_qpair_process_completions(ctx->qp[spdk_env_get_current_core()], 0);
        usleep(1000);
    }

    return 0;
}

int main(int argc, char **argv)
{
    struct spdk_env_opts opts;
    struct app_context ctx;

    spdk_env_opts_init(&opts);
    opts.name = "spdk_multi_core_example";
    opts.core_mask = "0x3"; // Core 0 和 Core 1
    spdk_env_init(&opts);

    // 探測 NVMe 控制器
    ctx.ctrlr = spdk_nvme_connect(NULL, 0, 0, NULL);
    if (!ctx.ctrlr) {
        fprintf(stderr, "Failed to connect NVMe controller\n");
        return -1;
    }

    // 啟動 reactor thread
    for (int i = 0; i < REACTOR_CORES; i++) {
        spdk_env_thread_launch_pinned(i, reactor_fn, &ctx);
    }

    // 主 thread 也進入 poll loop
    while (true) {
        usleep(1000);
    }

    return 0;
}

