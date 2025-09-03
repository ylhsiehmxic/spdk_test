#include "spdk/env.h"
#include "spdk/thread.h"
#include "spdk/nvme.h"
#include "spdk/nvme_intel.h"
#include "spdk/bdev.h"
#include "spdk/log.h"

static struct spdk_nvme_ctrlr *g_ctrlr = NULL;

/* 簡單的 NVMe 探測 callback */
static bool probe_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
                     struct spdk_nvme_ctrlr_opts *opts) {
    SPDK_NOTICELOG("Found NVMe controller at %s\n", trid->traddr);
    return true;
}

/* attach 時把 controller 存起來 */
static void attach_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
                      struct spdk_nvme_ctrlr *ctrlr,
                      const struct spdk_nvme_ctrlr_opts *opts) {
    SPDK_NOTICELOG("Attached to NVMe controller at %s\n", trid->traddr);
    g_ctrlr = ctrlr;
}

int main(int argc, char **argv) {
    struct spdk_env_opts opts;
    spdk_env_opts_init(&opts);

    /* 設定允許使用的 cores，例如 1,2,3 */
    opts.reactor_mask = "0x0E";   // binary 1110 → cores 1,2,3
    opts.name = "spdk_nvme_app";

    if (spdk_env_init(&opts) < 0) {
        SPDK_ERRLOG("Unable to initialize SPDK env\n");
        return -1;
    }

    /* 建立三個 reactor thread (分別跑在 core1, core2, core3) */
    struct spdk_cpuset cpumask;
    SPDK_ENV_FOREACH_CORE(core) {
        struct spdk_cpuset core_mask;
        spdk_cpuset_zero(&core_mask);
        spdk_cpuset_set_cpu(&core_mask, core, true);

        struct spdk_thread *thread = spdk_thread_create(NULL, &core_mask);
        if (!thread) {
            SPDK_ERRLOG("Failed to create thread on core %d\n", core);
            continue;
        }
        SPDK_NOTICELOG("Created SPDK thread bound to core %d\n", core);
    }

    /* 掃描並 attach NVMe controller */
    struct spdk_nvme_transport_id trid = {};
    spdk_nvme_trid_populate_transport(&trid, SPDK_NVME_TRANSPORT_PCIE);
    snprintf(trid.traddr, sizeof(trid.traddr), "%s", "0000:5e:00.0");

    if (spdk_nvme_probe(&trid, NULL, probe_cb, attach_cb, NULL) != 0) {
        SPDK_ERRLOG("spdk_nvme_probe() failed\n");
        return -1;
    }

    if (g_ctrlr) {
        /* 為每個 SPDK thread 建立一個 queue pair */
        struct spdk_nvme_qpair *qp;
        struct spdk_thread *thread = spdk_get_thread();

        qp = spdk_nvme_ctrlr_alloc_io_qpair(g_ctrlr, NULL, 0);
        if (!qp) {
            SPDK_ERRLOG("Failed to alloc IO qpair\n");
        } else {
            SPDK_NOTICELOG("Created IO qpair for thread %s on NVMe %s\n",
                           spdk_thread_get_name(thread),
                           spdk_nvme_ctrlr_get_transport_id(g_ctrlr)->traddr);
        }
    }

    spdk_env_fini();
    return 0;
}

