/*
單一 reactor 上有多個 SPDK threads（每個 thread 都各自拿到一條 bdev io_channel → 對應到底層各自的 NVMe qpair）

註：同一 reactor 下的多 SPDK thread 不會發生真正的 CPU context switch，是協作式輪詢，不像 OS 內核 thread 那樣上下文切換
*/
#include "spdk/stdinc.h"
#include "spdk/env.h"
#include "spdk/event.h"
#include "spdk/bdev.h"
#include "spdk/thread.h"

#define THREADS_PER_REACTOR  3     // 同一個 reactor 上要建立的 threads 數
#define IO_PER_THREAD        8     // 每個 thread 送出的 IO（read）數
#define BDEV_NAME            "Nvme0n1"  // 依你的環境調整

struct thread_ctx {
    struct spdk_thread      *th;
    struct spdk_bdev_desc   *desc;
    struct spdk_io_channel  *ch;
    struct spdk_bdev        *bdev;
    uint64_t                 submitted;
    uint64_t                 completed;
    char                     name[32];
};

struct io_task {
    struct thread_ctx *tctx;
    void *buf;
};

static struct thread_ctx g_ctx[THREADS_PER_REACTOR];
static struct spdk_bdev_desc *g_desc = NULL;
static uint64_t g_total_expected = 0;
static uint64_t g_total_completed = 0;

static void
io_complete(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
    struct io_task *task = cb_arg;
    struct thread_ctx *t = task->tctx;

    t->completed++;
    __atomic_fetch_add(&g_total_completed, 1, __ATOMIC_RELAXED);

    printf("[%-10s] I/O completed: %s  (thread: %lu/%lu, total: %lu/%lu)\n",
           spdk_thread_get_name(spdk_get_thread()),
           success ? "OK" : "FAIL",
           t->completed, (uint64_t)IO_PER_THREAD,
           g_total_completed, g_total_expected);

    spdk_bdev_free_io(bdev_io);
    spdk_free(task->buf);
    free(task);

    if (g_total_completed == g_total_expected) {
        spdk_app_stop(0);
    }
}

static void
submit_one_io(struct thread_ctx *t, uint64_t lba, uint32_t num_blocks)
{
    struct io_task *task = calloc(1, sizeof(*task));
    if (!task) {
        fprintf(stderr, "[%s] calloc io_task failed\n", t->name);
        return;
    }
    task->tctx = t;

    uint32_t bsz = spdk_bdev_get_block_size(t->bdev);
    task->buf = spdk_zmalloc((size_t)bsz * num_blocks, 0x1000, NULL,
                             SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
    if (!task->buf) {
        fprintf(stderr, "[%s] spdk_zmalloc failed\n", t->name);
        free(task);
        return;
    }

    int rc = spdk_bdev_read(t->desc, t->ch, task->buf,
                            lba * bsz, bsz * num_blocks,
                            io_complete, task);
    if (rc == 0) {
        t->submitted++;
        printf("[%-10s] submit READ  lba=%" PRIu64 " blocks=%u (submitted %lu/%u)\n",
               t->name, lba, num_blocks, t->submitted, (uint32_t)IO_PER_THREAD);
    } else {
        fprintf(stderr, "[%s] spdk_bdev_read submit failed rc=%d\n", t->name, rc);
        spdk_free(task->buf);
        free(task);
    }
}

static void
thread_work(void *arg)
{
    struct thread_ctx *t = arg;
    t->bdev = spdk_bdev_desc_get_bdev(t->desc);

    t->ch = spdk_bdev_get_io_channel(t->desc);
    if (!t->ch) {
        fprintf(stderr, "[%s] get_io_channel failed\n", t->name);
        return;
    }

    uint64_t nb  = spdk_bdev_get_num_blocks(t->bdev);
    uint32_t bsz = spdk_bdev_get_block_size(t->bdev);
    (void)bsz;

    printf("[%-10s] start on reactor core %d, nb=%" PRIu64 " blocks, bsz=%u\n",
           t->name, spdk_env_get_current_core(), nb, bsz);

    /* 送出多個 read IO；這裡用簡單的遞增 LBA（也可改成隨機） */
    for (uint32_t i = 0; i < IO_PER_THREAD; ++i) {
        uint64_t lba = (i % (nb ? nb : 1));   // 保證在範圍內
        submit_one_io(t, lba, 1);
    }
}

static void
app_start(void *arg)
{
    (void)arg;
    struct spdk_bdev *bdev = NULL;
    int rc = spdk_bdev_open_ext(BDEV_NAME, true, NULL, NULL, &g_desc);
    if (rc != 0) {
        fprintf(stderr, "open bdev %s failed rc=%d\n", BDEV_NAME, rc);
        spdk_app_stop(-1);
        return;
    }
    bdev = spdk_bdev_desc_get_bdev(g_desc);

    g_total_expected = THREADS_PER_REACTOR * IO_PER_THREAD;
    g_total_completed = 0;

    /* 在同一個 reactor（因為 reactor_mask=0x1）上建立多個 SPDK threads */
    for (int i = 0; i < THREADS_PER_REACTOR; ++i) {
        snprintf(g_ctx[i].name, sizeof(g_ctx[i].name), "t%d", i);
        g_ctx[i].th   = spdk_thread_create(g_ctx[i].name, NULL);
        g_ctx[i].desc = g_desc;
        g_ctx[i].bdev = bdev;
        g_ctx[i].submitted = g_ctx[i].completed = 0;

        /* 把工作投遞到該 thread 執行（同一個 reactor 的 loop 會輪流跑它們） */
        spdk_thread_send_msg(g_ctx[i].th, thread_work, &g_ctx[i]);
    }
}

int main(int argc, char **argv)
{
    struct spdk_app_opts opts;
    spdk_app_opts_init(&opts, sizeof(opts));
    opts.name = "bdev_reactor_multi_threads";
    opts.reactor_mask = "0x1";  // 單一 reactor（core0）

    int rc = spdk_app_start(&opts, app_start, NULL);
    if (rc) fprintf(stderr, "spdk_app_start rc=%d\n", rc);
    spdk_app_fini();
    return rc;
}
