/*
多核 reactor，每個 reactor 多 thread，每個 thread 多 qpair
*/
#include "spdk/stdinc.h"
#include "spdk/env.h"
#include "spdk/nvme.h"

#define REACTOR_CORES   2
#define THREADS_PER_REACTOR  2
#define QPAIRS_PER_THREAD    2
#define IO_PER_QP            4
#define NAMESPACE_ID         1

struct qpair_ctx {
    struct spdk_nvme_qpair *qpair;
    int id;
};

struct thread_ctx {
    struct spdk_nvme_ctrlr *ctrlr;
    struct qpair_ctx qpairs[QPAIRS_PER_THREAD];
    char name[32];
};

struct reactor_ctx {
    int core;
    struct thread_ctx threads[THREADS_PER_REACTOR];
};

static void io_complete(void *arg, const struct spdk_nvme_cpl *cpl) {
    struct qpair_ctx *qp = arg;
    printf("[%-10s] qpair %d I/O completed, status=0x%x\n",
           spdk_thread_get_name(spdk_get_thread()), qp->id, cpl->status.sc);
}

static void thread_work(void *arg) {
    struct thread_ctx *t = arg;
    struct spdk_nvme_ns *ns = spdk_nvme_ctrlr_get_ns(t->ctrlr, NAMESPACE_ID);

    printf("[%-10s] submitting IO on %d qpairs\n", t->name, QPAIRS_PER_THREAD);
    for (int i = 0; i < QPAIRS_PER_THREAD; i++) {
        struct qpair_ctx *qp = &t->qpairs[i];
        void *buf = spdk_zmalloc(spdk_nvme_ns_get_sector_size(ns),
                                 0x1000, NULL, SPDK_ENV_LCORE_ID_ANY, SPDK_MALLOC_DMA);
        for (int j = 0; j < IO_PER_QP; j++) {
            spdk_nvme_ns_cmd_read(ns, qp->qpair, buf, j, 1, io_complete, qp, 0);
        }
    }
}

static int init_thread(struct spdk_nvme_ctrlr *ctrlr, struct thread_ctx *t, const char *name) {
    t->ctrlr = ctrlr;
    snprintf(t->name, sizeof(t->name), "%s", name);
    for (int i = 0; i < QPAIRS_PER_THREAD; i++) {
        t->qpairs[i].id = i;
        t->qpairs[i].qpair = spdk_nvme_ctrlr_alloc_io_qpair(ctrlr, NULL, 0);
        if (!t->qpairs[i].qpair) {
            fprintf(stderr, "alloc qpair %d failed\n", i);
            return -1;
        }
    }
    return 0;
}

static void reactor_start(void *arg) {
    struct reactor_ctx *rctx = arg;
    for (int t = 0; t < THREADS_PER_REACTOR; t++) {
        char tname[32];
        snprintf(tname, sizeof(tname), "r%d_t%d", rctx->core, t);
        init_thread(rctx->threads + t, rctx->threads[t].name, tname);
        spdk_thread_send_msg(spdk_get_thread(), thread_work, rctx->threads + t);
    }
}

int main(int argc, char **argv) {
    struct spdk_app_opts opts;
    struct spdk_nvme_ctrlr *ctrlr;
    struct spdk_nvme_transport_id trid = {};
    struct reactor_ctx reactors[REACTOR_CORES];

    spdk_app_opts_init(&opts, sizeof(opts));
    opts.name = "nvme_multi_reactor_thread_qpair";
    opts.reactor_mask = "0x3"; // core0 & core1
    spdk_env_init(&opts);

    trid.trtype = SPDK_NVME_TRANSPORT_PCIE;
    snprintf(trid.traddr, sizeof(trid.traddr), "0000:01:00.0");

    ctrlr = spdk_nvme_connect(&trid, NULL, 0);
    if (!ctrlr) {
        fprintf(stderr, "connect NVMe ctrlr failed\n");
        return -1;
    }

    for (int core = 0; core < REACTOR_CORES; core++) {
        reactors[core].core = core;
        reactor_start(&reactors[core]);
    }

    /* Polling loop (簡化示範) */
    while (1) {
        for (int core = 0; core < REACTOR_CORES; core++) {
            for (int t = 0; t < THREADS_PER_REACTOR; t++) {
                for (int q = 0; q < QPAIRS_PER_THREAD; q++) {
                    spdk_nvme_qpair_process_completions(
                        reactors[core].threads[t].qpairs[q].qpair, 0);
                }
            }
        }
    }

    spdk_nvme_detach(ctrlr);
    return 0;
}
