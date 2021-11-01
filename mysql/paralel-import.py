#!/usr/bin/python3

#
# @author lukas.kasic
# @license mit
#

THREADS = 4

IMPORT_DIR = "/root/backupSQL/"
EXCLUDE = [
    "mysql",
    "information_schema",
    "performance_schema",
    "sys",
]

import time
import random
import os
import sys
import threading
import logging
import subprocess

from multiprocessing.dummy import Pool
logging.basicConfig(stream=sys.stdout, format="%(threadName)s:%(message)s", level=logging.INFO)

cleanup_pool = Pool(THREADS)
pool = Pool(THREADS)
db_pool = set()
db_count = 0
db_done = 0
start = None

def importing_thread(db):
    import_start = time.time()
    
    file = os.path.join(IMPORT_DIR, db + ".sql.gz")
    size = os.path.getsize(file)

    logging.warning("Importing DB=%s Size=%0.2f MB" % (db, size / 1024.0 / 1024.0))

    cmd = "mysql -e 'create database if not exists %s'" % db
    rc = subprocess.call(cmd, shell=True)

    if rc == 0:
        cmd = "zcat %s | sed 's/ENGINE=MyISAM/ENGINE=InnoDB/g' | mysql %s" % (file, db)
        rc = subprocess.call(cmd, shell=True)

    import_end = time.time()

    global db_done
    db_done += 1
    result = { "database": db, "time": import_end-import_start, "rc": rc}
    logging.warning(result)

    perc = 100.0 * db_done / db_count
    elapsed = int(import_end-start)
    total = int((db_count/db_done) * elapsed)
    logging.warning("done: %0.2f%% (elapsed %ds, total = %ds)" % (perc, elapsed, total) )
    return result

def drop_db(db):
    logging.warning("Dropping database %s" % db)
    cmd = "mysql -e 'drop database if exists %s'" % db
    rc = subprocess.call(cmd, shell=True)


if __name__ == "__main__":
    files = os.listdir(IMPORT_DIR)

    for file in files:
        dbname = file.replace(".sql.gz", "")
        if dbname in EXCLUDE:
            continue
        db_pool.add(dbname)

    db_count = len(db_pool)

    start = time.time()
    cleanup_pool.map(drop_db, db_pool)
    cleanup_pool.close()
    cleanup_pool.join()
    end = time.time()
    print("Cleanup time: %f" % (end-start))

    start = time.time()
    results = pool.map(importing_thread, db_pool)

    pool.close()
    pool.join()

    end = time.time()
    print("Import time: %f" % (end-start))

    #print(results)


