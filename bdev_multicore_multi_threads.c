/*
多核 reactor，每個 reactor 多 thread，每個 thread 一個 qpair
*/
#include "spdk/stdinc.h"
#include "spdk/env.h"
#include "spdk/event.h"
#include "spdk/bdev.h"
#include "spdk/thread.h"

#define THREADS_PER_REACTOR  2
#define IO_PER_THREAD        4
#define BDEV_NAME            "Nvme0n1"

struct thread_ctx {
    struct spdk_thread *th;
    struct spdk_bdev_desc *desc;
    struct spdk_io_channel *ch;
    struct spdk_bdev *bdev;
    char name[32];
};

struct thread_ctx g_ctx[THREADS_PER_REACTOR*2]; // 2 reactor * THREADS_PER_REACTOR
static struct spdk_bdev_desc *g_desc;
static uint64_t g_total_expected, g_total_completed;

static void io_complete(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg) {
    (void)bdev_io; (void)success;
    g_total_completed++;
    if (g_total_completed == g_total_expected) {
        spdk_app_stop(0);
    }
}

static void submit_io(struct thread_ctx *t) {
    struct spdk_bdev *bdev = t->bdev;
    for (uint32_t i = 0; i < IO_PER_THREAD; ++i) {
        void *buf = spdk_zmalloc(spdk_bdev_get_block_size(bdev), 0x1000, NULL,
                                 SPDK_ENV_LCORE_ID_ANY, SPDK_MALLOC_DMA);
        spdk_bdev_read(t->desc, t->ch, buf, i, 1, io_complete, NULL);
    }
}

static void thread_work(void *arg) {
    struct thread_ctx *t = arg;
    t->bdev = spdk_bdev_desc_get_bdev(t->desc);
    t->ch = spdk_bdev_get_io_channel(t->desc);
    submit_io(t);
}

static void app_start(void *arg) {
    (void)arg;
    int rc = spdk_bdev_open_ext(BDEV_NAME, true, NULL, NULL, &g_desc);
    if (rc) { spdk_app_stop(-1); return; }

    g_total_expected = THREADS_PER_REACTOR*2 * IO_PER_THREAD;
    g_total_completed = 0;

    int idx = 0;
    for (int core = 0; core < 2; core++) { // reactor/core0, core1
        for (int t = 0; t < THREADS_PER_REACTOR; t++) {
            snprintf(g_ctx[idx].name, sizeof(g_ctx[idx].name), "r%d_t%d", core, t);
            g_ctx[idx].th = spdk_thread_create(g_ctx[idx].name, NULL);
            g_ctx[idx].desc = g_desc;
            spdk_thread_send_msg(g_ctx[idx].th, thread_work, &g_ctx[idx]);
            idx++;
        }
    }
}

int main(int argc, char **argv) {
    struct spdk_app_opts opts;
    spdk_app_opts_init(&opts, sizeof(opts));
    opts.name = "bdev_multicore_multi_threads";
    opts.reactor_mask = "0x3"; // core0 & core1
    spdk_app_start(&opts, app_start, NULL);
    spdk_app_fini();
    return 0;
}
