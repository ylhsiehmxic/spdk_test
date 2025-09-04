/*
多核 reactor，每個 reactor 一個 thread 控多個 qpair
*/
#include "spdk/stdinc.h"
#include "spdk/env.h"
#include "spdk/nvme.h"

#define NUM_QPAIR   4
#define IO_PER_QP   4
#define NAMESPACE_ID 1

struct qpair_ctx {
    struct spdk_nvme_qpair *qpair;
    int id;
};

struct thread_ctx {
    struct spdk_nvme_ctrlr *ctrlr;
    struct qpair_ctx qpairs[NUM_QPAIR];
};

static void io_complete(void *arg, const struct spdk_nvme_cpl *cpl) {
    struct qpair_ctx *qp = arg;
    (void)cpl;
    printf("[Thread %s] qpair %d I/O completed\n",
           spdk_thread_get_name(spdk_get_thread()), qp->id);
}

static void thread_work(void *arg) {
    struct thread_ctx *t = arg;
    struct spdk_nvme_ns *ns = spdk_nvme_ctrlr_get_ns(t->ctrlr, NAMESPACE_ID);

    for (int i = 0; i < NUM_QPAIR; ++i) {
        struct qpair_ctx *qp = &t->qpairs[i];
        void *buf = spdk_zmalloc(spdk_nvme_ns_get_sector_size(ns),
                                 0x1000, NULL, SPDK_ENV_LCORE_ID_ANY, SPDK_MALLOC_DMA);
        for (int j = 0; j < IO_PER_QP; ++j) {
            spdk_nvme_ns_cmd_read(ns, qp->qpair, buf, j, 1, io_complete, qp, 0);
        }
    }
}

static int init_ctrlr(struct spdk_nvme_ctrlr *ctrlr, struct thread_ctx *t) {
    t->ctrlr = ctrlr;
    for (int i = 0; i < NUM_QPAIR; i++) {
        t->qpairs[i].id = i;
        t->qpairs[i].qpair = spdk_nvme_ctrlr_alloc_io_qpair(ctrlr, NULL, 0);
        if (!t->qpairs[i].qpair) return -1;
    }
    return 0;
}

static void app_start(void *arg) {
    (void)arg;
    struct spdk_nvme_ctrlr *ctrlr;
    struct spdk_nvme_transport_id trid = {};
    struct thread_ctx *tctx[2];

    trid.trtype = SPDK_NVME_TRANSPORT_PCIE;
    snprintf(trid.traddr, sizeof(trid.traddr), "0000:01:00.0");
    ctrlr = spdk_nvme_connect(&trid, NULL, 0);
    if (!ctrlr) return;

    for (int core = 0; core < 2; core++) {
        tctx[core] = malloc(sizeof(struct thread_ctx));
        init_ctrlr(ctrlr, tctx[core]);
        spdk_event_call(spdk_event_allocate(core, (spdk_event_fn)thread_work, tctx[core], NULL));
    }
}

int main(int argc, char **argv) {
    struct spdk_app_opts opts;
    spdk_app_opts_init(&opts, sizeof(opts));
    opts.name = "nvme_multicore_multi_qpair";
    opts.reactor_mask = "0x3"; // core0 & core1
    spdk_app_start(&opts, app_start, NULL);
    spdk_app_fini();
    return 0;
}
