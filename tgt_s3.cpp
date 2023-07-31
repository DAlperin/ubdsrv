#include <config.h>

#include <poll.h>
#include <sys/epoll.h>
#include "ublksrv_tgt.h"
#include "ublksrv_aio.h"
#include "error.h"

#include <aws/core/Aws.h>
#include <aws/core/utils/logging/LogLevel.h>
#include <aws/s3/S3Client.h>

const int SECTOR_SIZE = 512;

struct object_entry {
    unsigned offset;
    unsigned id;
};

struct log_item {
    unsigned long long lba;
    char* buf;
};

static auto sector_to_object = std::map<unsigned, object_entry>();
static auto sector_to_log = std::vector<log_item>();
static struct ublksrv_aio_ctx *aio_ctx = nullptr;
static const struct ublksrv_dev *this_dev;
static pthread_t io_thread;


static pthread_mutex_t jbuf_lock;
static char jbuf[4096];


struct s3_queue_info {
    const struct ublksrv_dev *dev;
    const struct ublksrv_queue *q;
    int qid;

    pthread_t thread;
};

//static int restore_from_s3() {
//    Aws::SDKOptions options;
//    options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Debug;
//}

static int s3_handle_io_async(const struct ublksrv_queue *q, const struct ublk_io_data *data) {

    struct ublksrv_aio *req = ublksrv_aio_alloc_req(aio_ctx, 0);

    req->io = *data->iod;
    req->id = ublksrv_aio_pid_tag(q->q_id, data->tag);
    ublksrv_aio_submit_req(aio_ctx, q, req);

    return 0;
}


int sync_io_submitter(struct ublksrv_aio_ctx *ctx,
                      struct ublksrv_aio *req)
{
    const struct ublksrv_io_desc *iod = &req->io;
    unsigned ublk_op = ublksrv_get_op(iod);
    void *buf = (void *)iod->addr;
    unsigned len = iod->nr_sectors << 9;
    unsigned long long offset = iod->start_sector << 9;
    int mode = FALLOC_FL_KEEP_SIZE;
    int ret;

    unsigned long long lba = offset / SECTOR_SIZE;

    switch (ublk_op) {
        case UBLK_IO_OP_READ: {
            auto it = sector_to_log.rbegin();
            while (it != sector_to_log.rend()) {
                auto end_lba = sizeof(it->buf)/SECTOR_SIZE;
                if (lba >= it->lba && lba <= end_lba) {
                    memcpy(buf, it->buf+offset, len);
                    break;
                }
            }
            break;
        }
        case UBLK_IO_OP_WRITE: {
            //Update local cache
            char* out_buf;
            memcpy(out_buf, (char*)buf+offset, len);
            struct log_item li = {
                    .lba = lba,
                    .buf = out_buf,
            };
            sector_to_log.push_back(li);
            break;
        }
        case UBLK_IO_OP_FLUSH:
            //Flush local cache to s3
            break;
        case UBLK_IO_OP_WRITE_ZEROES:
            mode |= FALLOC_FL_ZERO_RANGE;
        case UBLK_IO_OP_DISCARD:
            break;
        default:
            fprintf(stderr, "%s: wrong op %d, fd %d, id %x\n", __func__,
                    ublk_op, req->fd, req->id);
            return -EINVAL;
    }

    req->res = ret;
    return 1;
}

static int io_submit_worker(struct ublksrv_aio_ctx *ctx,
                            struct ublksrv_aio *req)
{
    return sync_io_submitter(ctx, req);
}

#define EPOLL_NR_EVENTS 1
static void *s3_event_real_io_handler_fn(void *data) {
    struct ublksrv_aio_ctx *ctx = (struct ublksrv_aio_ctx *)data;
    const struct ublksrv_dev *dev = ublksrv_aio_get_dev(ctx);
    const struct ublksrv_ctrl_dev_info *info =
            ublksrv_ctrl_get_dev_info(ublksrv_get_ctrl_dev(dev));

    unsigned dev_id = info->dev_id;
    struct epoll_event events[EPOLL_NR_EVENTS];
    int epoll_fd = epoll_create(EPOLL_NR_EVENTS);
    struct epoll_event read_event;
    int ctx_efd = ublksrv_aio_get_efd(ctx);

    if (epoll_fd < 0) {
        fprintf(stderr, "ublk dev %d create epoll fd failed\n", dev_id);
        return NULL;
    }

    fprintf(stdout, "ublk dev %d aio context(sync io submitter) started tid %d\n",
            dev_id, ublksrv_gettid());

    read_event.events = EPOLLIN;
    read_event.data.fd = ctx_efd;
    (void)epoll_ctl(epoll_fd, EPOLL_CTL_ADD, ctx_efd, &read_event);

    while (!ublksrv_aio_ctx_dead(ctx)) {
        struct aio_list completions;

        aio_list_init(&completions);

        ublksrv_aio_submit_worker(ctx, io_submit_worker, &completions);

        ublksrv_aio_complete_worker(ctx, &completions);

        epoll_wait(epoll_fd, events, EPOLL_NR_EVENTS, -1);
    }

    return NULL;
}


static void *demo_event_io_handler_fn(void *data)
{
    struct s3_queue_info *info = (struct s3_queue_info*)data;
    const struct ublksrv_dev *dev = info->dev;
    const struct ublksrv_ctrl_dev_info *dinfo =
            ublksrv_ctrl_get_dev_info(ublksrv_get_ctrl_dev(dev));
    unsigned dev_id = dinfo->dev_id;
    unsigned short q_id = info->qid;
    const struct ublksrv_queue *q;

    pthread_mutex_lock(&jbuf_lock);
    ublksrv_json_write_queue_info(ublksrv_get_ctrl_dev(dev), jbuf, sizeof jbuf,
                                  q_id, ublksrv_gettid());
    pthread_mutex_unlock(&jbuf_lock);

    q = ublksrv_queue_init(dev, q_id, info);
    if (!q) {
        fprintf(stderr, "ublk dev %d queue %d init queue failed\n",
                dinfo->dev_id, q_id);
        return NULL;
    }
    info->q = q;

    fprintf(stdout, "tid %d: ublk dev %d queue %d started\n", ublksrv_gettid(),
            dev_id, q->q_id);
    do {
        if (ublksrv_process_io(q) < 0)
            break;
    } while (1);

    fprintf(stdout, "ublk dev %d queue %d exited\n", dev_id, q->q_id);
    ublksrv_queue_deinit(q);
    return NULL;
}


static int s3_event_io_handler(struct ublksrv_ctrl_dev *ctrl_dev) {
    const struct ublksrv_ctrl_dev_info *dinfo =
            ublksrv_ctrl_get_dev_info(ctrl_dev);
    int dev_id = dinfo->dev_id;
    int ret, i;
    const struct ublksrv_dev *dev;
    struct s3_queue_info *info_array;
    void *thread_ret;

    info_array = (struct s3_queue_info *)
            calloc(sizeof(struct s3_queue_info), dinfo->nr_hw_queues);
    if (!info_array)
        return -ENOMEM;


    dev = ublksrv_dev_init(ctrl_dev);
    if (!dev) {
        free(info_array);
        return -ENOMEM;
    }
    this_dev = dev;


    aio_ctx = ublksrv_aio_ctx_init(dev, 0);
    if (!aio_ctx) {
        fprintf(stderr, "dev %d call ublk_aio_ctx_init failed\n", dev_id);
        ret = -ENOMEM;
        goto fail;
    }

    pthread_create(&io_thread, NULL, s3_event_real_io_handler_fn,
                   aio_ctx);


    for (i = 0; i < dinfo->nr_hw_queues; i++) {
        info_array[i].dev = dev;
        info_array[i].qid = i;

        pthread_create(&info_array[i].thread, NULL,
                       demo_event_io_handler_fn,
                       &info_array[i]);
    }

    fail:
        ublksrv_dev_deinit(dev);
        free(info_array);
        return ret;
}

static void s3_handle_event(const struct ublksrv_queue *q)
{
    ublksrv_aio_handle_event(aio_ctx, q);
}


static int s3_init_tgt(struct ublksrv_dev *dev, int type, int argc,
                         char *argv[])
{
    const struct ublksrv_ctrl_dev_info *info =
            ublksrv_ctrl_get_dev_info(ublksrv_get_ctrl_dev(dev));
    struct ublksrv_tgt_info *tgt = &dev->tgt;
    struct ublksrv_tgt_base_json tgt_json = {
            .type = type,
    };

    strcpy(tgt_json.name, "s3");

    tgt->dev_size = 250UL * 1024 * 1024 * 1024;

    tgt_json.dev_size = tgt->dev_size;
    tgt->tgt_ring_depth = info->queue_depth;

    // We aren't using the io_uring from ublksrv
    tgt->nr_fds = 0;

    ublksrv_json_write_dev_info(ublksrv_get_ctrl_dev(dev), jbuf, sizeof jbuf);
    ublksrv_json_write_target_base_info(jbuf, sizeof jbuf, &tgt_json);

    return 0;
}

static const struct ublksrv_tgt_type s3_tgt_type = {
        .handle_io_async = s3_handle_io_async,
        .handle_event = s3_handle_event,
        .init_tgt = s3_init_tgt,
        .type = UBLKSRV_TGT_TYPE_S3,
        .name = "s3",
};


static int ublksrv_start_daemon(struct ublksrv_ctrl_dev *ctrl_dev)
{
    if (ublksrv_ctrl_get_affinity(ctrl_dev) < 0)
        return -1;

    return s3_event_io_handler(ctrl_dev);
}

int main(int argc, char *argv[])
{
    struct ublksrv_dev_data data = {
            .dev_id = -1,
            .max_io_buf_bytes = DEF_BUF_SIZE,
            .nr_hw_queues = DEF_NR_HW_QUEUES,
            .queue_depth = DEF_QD,
            .tgt_type = "s3",
            .tgt_ops = &s3_tgt_type,
            .flags = 0,
    };
    struct ublksrv_ctrl_dev *dev;
    int ret, opt;

    pthread_mutex_init(&jbuf_lock, NULL);

    data.ublksrv_flags = UBLKSRV_F_NEED_EVENTFD;
    dev = ublksrv_ctrl_init(&data);
    if (!dev)
        error(EXIT_FAILURE, ENODEV, "ublksrv_ctrl_init");

    ret = ublksrv_ctrl_add_dev(dev);
    if (ret < 0) {
        error(0, -ret, "can't add dev %d", data.dev_id);
        goto fail;
    }

    ret = ublksrv_start_daemon(dev);
    if (ret < 0) {
        error(0, -ret, "can't start daemon");
        goto fail_del_dev;
    }

    ublksrv_ctrl_del_dev(dev);
    ublksrv_ctrl_deinit(dev);
    exit(EXIT_SUCCESS);

    fail_del_dev:
    ublksrv_ctrl_del_dev(dev);
    fail:
    ublksrv_ctrl_deinit(dev);

    exit(EXIT_FAILURE);
}
