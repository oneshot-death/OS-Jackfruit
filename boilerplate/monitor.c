
#include <linux/cdev.h>
#include <linux/device.h>
#include <linux/fs.h>
#include <linux/init.h>
#include <linux/kernel.h>
#include <linux/list.h>
#include <linux/mm.h>
#include <linux/module.h>
#include <linux/mutex.h>
#include <linux/pid.h>
#include <linux/sched/signal.h>
#include <linux/slab.h>
#include <linux/spinlock.h>
#include <linux/timer.h>
#include <linux/uaccess.h>
#include <linux/version.h>

#include "monitor_ioctl.h"

#define DEVICE_NAME        "container_monitor"
#define CHECK_INTERVAL_SEC 1

/* ==========================================================================
 * TODO 1 – Monitored entry struct
 * ========================================================================== */
struct monitored_entry {
    pid_t         pid;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int           soft_warned;          /* 1 after first soft-limit warning */
    char          container_id[32];
    struct list_head list;
};

/* ==========================================================================
 * TODO 2 – Global list and spinlock
 * ========================================================================== */
static LIST_HEAD(monitored_list);
static DEFINE_SPINLOCK(monitored_lock);

/* --- Provided: device / timer state --------------------------------------- */
static struct timer_list monitor_timer;
static dev_t              dev_num;
static struct cdev        c_dev;
static struct class      *cl;

/* --- Provided: RSS helper ------------------------------------------------- */
static long get_rss_bytes(pid_t pid)
{
    struct task_struct *task;
    struct mm_struct   *mm;
    long rss_pages = 0;

    rcu_read_lock();
    task = pid_task(find_vpid(pid), PIDTYPE_PID);
    if (!task) { rcu_read_unlock(); return -1; }
    get_task_struct(task);
    rcu_read_unlock();

    mm = get_task_mm(task);
    if (mm) { rss_pages = get_mm_rss(mm); mmput(mm); }
    put_task_struct(task);
    return rss_pages * PAGE_SIZE;
}

/* --- Provided: soft-limit event ------------------------------------------ */
static void log_soft_limit_event(const char *container_id, pid_t pid,
                                 unsigned long limit_bytes, long rss_bytes)
{
    printk(KERN_WARNING
           "[container_monitor] SOFT LIMIT container=%s pid=%d rss=%ld limit=%lu\n",
           container_id, pid, rss_bytes, limit_bytes);
}

/* --- Provided: hard-limit event ------------------------------------------ */
static void kill_process(const char *container_id, pid_t pid,
                         unsigned long limit_bytes, long rss_bytes)
{
    struct task_struct *task;
    rcu_read_lock();
    task = pid_task(find_vpid(pid), PIDTYPE_PID);
    if (task) send_sig(SIGKILL, task, 1);
    rcu_read_unlock();
    printk(KERN_WARNING
           "[container_monitor] HARD LIMIT container=%s pid=%d rss=%ld limit=%lu\n",
           container_id, pid, rss_bytes, limit_bytes);
}

/* ==========================================================================
 * TODO 3 – Timer callback: periodic RSS check
 * ========================================================================== */
static void timer_callback(struct timer_list *t)
{
    struct monitored_entry *entry, *tmp;
    unsigned long flags;

    spin_lock_irqsave(&monitored_lock, flags);
    list_for_each_entry_safe(entry, tmp, &monitored_list, list) {
        pid_t         pid        = entry->pid;
        unsigned long soft       = entry->soft_limit_bytes;
        unsigned long hard       = entry->hard_limit_bytes;
        int           warned     = entry->soft_warned;
        char          cid[32];
        strncpy(cid, entry->container_id, sizeof(cid) - 1);
        cid[sizeof(cid)-1] = '\0';

        /* Release lock before any operations that may schedule */
        spin_unlock_irqrestore(&monitored_lock, flags);

        long rss = get_rss_bytes(pid);

        spin_lock_irqsave(&monitored_lock, flags);

        if (rss < 0) {
            /* Process gone — remove stale entry */
            printk(KERN_INFO "[container_monitor] removing stale entry container=%s pid=%d\n",
                   cid, pid);
            list_del(&entry->list);
            kfree(entry);
            continue;
        }

        if ((unsigned long)rss > hard) {
            /* Hard limit: kill and remove */
            spin_unlock_irqrestore(&monitored_lock, flags);
            kill_process(cid, pid, hard, rss);
            spin_lock_irqsave(&monitored_lock, flags);
            /* Find entry again after re-acquiring lock; it may have been
             * removed by a concurrent UNREGISTER ioctl, so search by pid. */
            {
                struct monitored_entry *e2, *t2;
                list_for_each_entry_safe(e2, t2, &monitored_list, list) {
                    if (e2->pid == pid) {
                        list_del(&e2->list);
                        kfree(e2);
                        break;
                    }
                }
            }
        } else if ((unsigned long)rss > soft && !warned) {
            /* Soft limit: warn once */
            entry->soft_warned = 1;
            spin_unlock_irqrestore(&monitored_lock, flags);
            log_soft_limit_event(cid, pid, soft, rss);
            spin_lock_irqsave(&monitored_lock, flags);
        }
    }
    spin_unlock_irqrestore(&monitored_lock, flags);

    mod_timer(&monitor_timer, jiffies + CHECK_INTERVAL_SEC * HZ);
}

/* ==========================================================================
 * IOCTL dispatch
 * ========================================================================== */
static long monitor_ioctl(struct file *f, unsigned int cmd, unsigned long arg)
{
    struct monitor_request req;
    unsigned long flags;

    (void)f;

    if (cmd != MONITOR_REGISTER && cmd != MONITOR_UNREGISTER)
        return -EINVAL;

    if (copy_from_user(&req, (struct monitor_request __user *)arg, sizeof(req)))
        return -EFAULT;

    if (cmd == MONITOR_REGISTER) {
        /* =================================================================
         * TODO 4 – Add a monitored entry
         * ================================================================= */
        struct monitored_entry *entry;

        if (req.soft_limit_bytes > req.hard_limit_bytes) {
            printk(KERN_WARNING "[container_monitor] REGISTER rejected: soft > hard\n");
            return -EINVAL;
        }

        entry = kzalloc(sizeof(*entry), GFP_KERNEL);
        if (!entry) return -ENOMEM;

        entry->pid              = req.pid;
        entry->soft_limit_bytes = req.soft_limit_bytes;
        entry->hard_limit_bytes = req.hard_limit_bytes;
        entry->soft_warned      = 0;
        strncpy(entry->container_id, req.container_id,
                sizeof(entry->container_id) - 1);
        INIT_LIST_HEAD(&entry->list);

        spin_lock_irqsave(&monitored_lock, flags);
        list_add(&entry->list, &monitored_list);
        spin_unlock_irqrestore(&monitored_lock, flags);

        printk(KERN_INFO
               "[container_monitor] Registered container=%s pid=%d soft=%lu hard=%lu\n",
               entry->container_id, entry->pid,
               entry->soft_limit_bytes, entry->hard_limit_bytes);
        return 0;
    }

    /* MONITOR_UNREGISTER ====================================================
     * TODO 5 – Remove a monitored entry
     * ======================================================================= */
    printk(KERN_INFO "[container_monitor] Unregister container=%s pid=%d\n",
           req.container_id, req.pid);
    {
        struct monitored_entry *entry, *tmp;
        int found = 0;

        spin_lock_irqsave(&monitored_lock, flags);
        list_for_each_entry_safe(entry, tmp, &monitored_list, list) {
            if (entry->pid == req.pid) {
                list_del(&entry->list);
                kfree(entry);
                found = 1;
                break;
            }
        }
        spin_unlock_irqrestore(&monitored_lock, flags);

        if (!found) {
            printk(KERN_WARNING "[container_monitor] Unregister: pid=%d not found\n",
                   req.pid);
            return -ENOENT;
        }
    }
    return 0;
}

/* --- File operations ------------------------------------------------------ */
static struct file_operations fops = {
    .owner          = THIS_MODULE,
    .unlocked_ioctl = monitor_ioctl,
};

/* --- Module init ---------------------------------------------------------- */
static int __init monitor_init(void)
{
    if (alloc_chrdev_region(&dev_num, 0, 1, DEVICE_NAME) < 0)
        return -1;

#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 4, 0)
    cl = class_create(DEVICE_NAME);
#else
    cl = class_create(THIS_MODULE, DEVICE_NAME);
#endif
    if (IS_ERR(cl)) { unregister_chrdev_region(dev_num, 1); return PTR_ERR(cl); }

    if (IS_ERR(device_create(cl, NULL, dev_num, NULL, DEVICE_NAME))) {
        class_destroy(cl); unregister_chrdev_region(dev_num, 1); return -1;
    }

    cdev_init(&c_dev, &fops);
    if (cdev_add(&c_dev, dev_num, 1) < 0) {
        device_destroy(cl, dev_num); class_destroy(cl);
        unregister_chrdev_region(dev_num, 1); return -1;
    }

    timer_setup(&monitor_timer, timer_callback, 0);
    mod_timer(&monitor_timer, jiffies + CHECK_INTERVAL_SEC * HZ);

    printk(KERN_INFO "[container_monitor] Module loaded. /dev/%s\n", DEVICE_NAME);
    return 0;
}

/* --- Module exit ---------------------------------------------------------- */
static void __exit monitor_exit(void)
{
    timer_delete_sync(&monitor_timer);

    /* =======================================================================
     * TODO 6 – Free all remaining monitored entries
     * ======================================================================= */
    {
        struct monitored_entry *entry, *tmp;
        unsigned long flags;

        spin_lock_irqsave(&monitored_lock, flags);
        list_for_each_entry_safe(entry, tmp, &monitored_list, list) {
            list_del(&entry->list);
            kfree(entry);
        }
        spin_unlock_irqrestore(&monitored_lock, flags);
    }

    cdev_del(&c_dev);
    device_destroy(cl, dev_num);
    class_destroy(cl);
    unregister_chrdev_region(dev_num, 1);

    printk(KERN_INFO "[container_monitor] Module unloaded.\n");
}

module_init(monitor_init);
module_exit(monitor_exit);

MODULE_LICENSE("GPL");
MODULE_DESCRIPTION("Supervised multi-container memory monitor");
