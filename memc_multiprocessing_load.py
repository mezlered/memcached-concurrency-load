#!/usr/bin/env python
# -*- coding: utf-8 -*-
import Queue
import collections
import glob
import gzip
import logging
import os
import sys
import threading
import time
from functools import partial
from optparse import OptionParser
from multiprocessing import Pool

import appsinstalled_pb2
import memcache


NORMAL_ERR_RATE = 0.01
COUNT_THREAD = 4
MAX_RETRIES = 3
MEMC_BACKOFF_FACTOR = 0.3
MEMCACHE_RETRY_TIMEOUT = 0.5
TIMEOUT_QUEUE = 0.1
DEAD_RETRY = 3
SOCKET_TIMEOUT = 3
MAX_COUNT_DATA_PACKED = 300
LINE_PARTS = 5

AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])


def dot_rename(path):
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, "." + fn))


def memcache_client_set_multi(memcache_client, data_packet, dry_run=False):
    memc_addr = memcache_client.servers[0].address

    if dry_run:
        logging.debug("%s - %s" % (memc_addr, data_packet.key()))
        return 0

    try:
        for n in range(MAX_RETRIES):
            set_multi = memcache_client.set_multi(data_packet)
            if len(set_multi) == 0:
                return 0
            backoff_value = MEMC_BACKOFF_FACTOR * (2 ** n)
            time.sleep(backoff_value)
        return len(set_multi)
    except Exception, e:
        logging.exception("Cannot write to memc %s: %s" % (memc_addr, e))
        return False


def parse_appsinstalled(line):
    line_parts = line.strip().split("\t")
    if len(line_parts) < LINE_PARTS:
        return
    dev_type, dev_id, lat, lon, raw_apps = line_parts
    if not dev_type or not dev_id:
        return
    try:
        apps = [int(a.strip()) for a in raw_apps.split(",")]
    except ValueError:
        apps = [int(a.strip()) for a in raw_apps.split(",") if a.isidigit()]
        logging.info("Not all user apps are digits: `%s`" % line)
    try:
        lat, lon = float(lat), float(lon)
    except ValueError:
        logging.info("Invalid geo coords: `%s`" % line)
    return AppsInstalled(dev_type, dev_id, lat, lon, apps)


def get_packed(appsinstalled):
    ua = appsinstalled_pb2.UserApps()
    ua.lat = appsinstalled.lat
    ua.lon = appsinstalled.lon
    key = "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)
    ua.apps.extend(appsinstalled.apps)
    packed = ua.SerializeToString()
    return {key: packed}


def handle_task(task_queue, result_queue):
    processed = errors = 0
    while True:
        try:
            task = task_queue.get(timeout=TIMEOUT_QUEUE)
        except Queue.Empty:
            result_queue.put((processed, errors))
            return
        memcache_client, data_packet, dry_run = task
        count_errs = memcache_client_set_multi(memcache_client, data_packet, dry_run)

        errors += count_errs
        processed += len(data_packet) - count_errs


def client_memcache_initialization(device_memc):
    memcache_client = {}
    for store, memc_addr in device_memc.items():
        memcache_client[store] = memcache.Client(
            [memc_addr],
            dead_retry=DEAD_RETRY, 
            socket_timeout=SOCKET_TIMEOUT,
        )
    return memcache_client


def process(fn, options):
    device_memc = {
        "idfa": options.idfa,
        "gaid": options.gaid,
        "adid": options.adid,
        "dvid": options.dvid,
    }

    memcache_clients = client_memcache_initialization(device_memc)
    task_queue = Queue.Queue()
    result_queue = Queue.Queue()
    data = collections.defaultdict(dict)

    workers = []
    for _ in range(COUNT_THREAD):
        thread = threading.Thread(target=handle_task, args=(task_queue, result_queue))
        thread.daemon = True
        workers.append(thread)

    for thread in workers:
        thread.start()

    logging.info('Processing %s' % fn)
    with gzip.open(fn) as fd:
        processed = errors = 0
        for line in fd:
            line = line.strip()
            if not line:
                continue

            appsinstalled = parse_appsinstalled(line)
            if not appsinstalled:
                errors += 1
                continue

            memc_addr = device_memc.get(appsinstalled.dev_type)
            if not memc_addr:
                errors += 1
                logging.error("Unknow device type: %s" % appsinstalled.dev_type)
                continue

            data[appsinstalled.dev_type].update(get_packed(appsinstalled))
            
            for indf in  data.keys():
                if len(data[indf]) == MAX_COUNT_DATA_PACKED:
                    task_queue.put((memcache_clients[indf], data[indf], options.dry))
                    data[indf] = dict()

        else:
             for indf in data.keys():
                task_queue.put((memcache_clients[indf], data[indf], options.dry))

    for thread in workers:
        if thread.is_alive():
            thread.join()

    for _ in range(result_queue.qsize()):
        processed_worker, errors_worker = result_queue.get()
        processed += processed_worker
        errors += errors_worker

    if processed:
        err_rate = float(errors) / processed
        if err_rate < NORMAL_ERR_RATE:
            logging.info("Acceptable error rate (%s). Successfull load" % err_rate)
        else:
            logging.error("High error rate (%s > %s). Failed load" % (err_rate, NORMAL_ERR_RATE))

    return fn


def main(options):
    pool = Pool()
    fnames = sorted(fn for fn in glob.iglob(options.pattern))

    handler = partial(process, options=options)
    for fn in pool.imap(handler, fnames, chunksize=options.chunksize):
        dot_rename(fn)


def prototest():
    sample = "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
    for line in sample.splitlines():
        dev_type, dev_id, lat, lon, raw_apps = line.strip().split("\t")
        apps = [int(a) for a in raw_apps.split(",") if a.isdigit()]
        lat, lon = float(lat), float(lon)
        ua = appsinstalled_pb2.UserApps()
        ua.lat = lat
        ua.lon = lon
        ua.apps.extend(apps)
        packed = ua.SerializeToString()
        unpacked = appsinstalled_pb2.UserApps()
        unpacked.ParseFromString(packed)
        assert ua == unpacked


if __name__ == '__main__':
    op = OptionParser()
    op.add_option("-t", "--test", action="store_true", default=False)
    op.add_option("-l", "--log", action="store", default=None)
    op.add_option("--dry", action="store_true", default=False)
    op.add_option("--pattern", action="store", default="/data/appsinstalled/*.tsv.gz")
    op.add_option("--chunksize", type="float", default=1.05)
    op.add_option("--idfa", action="store", default="127.0.0.1:33013")
    op.add_option("--gaid", action="store", default="127.0.0.1:33014")
    op.add_option("--adid", action="store", default="127.0.0.1:33015")
    op.add_option("--dvid", action="store", default="127.0.0.1:33016")
    (opts, args) = op.parse_args()
    logging.basicConfig(filename=opts.log, level=logging.INFO if not opts.dry else logging.DEBUG,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
    if opts.test:
        prototest()
        sys.exit(0)

    logging.info("Memc loader started with options: %s" % opts)
    try:
        main(opts)
    except Exception, e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)
