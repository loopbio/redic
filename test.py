from redic import Model, KeyScheme, IntKeyPart, StringKeyPart


class Progress(Model):
    prefix = 'progress:'

    percentage = KeyScheme(IntKeyPart('user', length=5),
                           IntKeyPart('job', length=5))
    preview = KeyScheme(IntKeyPart('user', length=5),
                        IntKeyPart('job', length=5))
    njobs = KeyScheme(IntKeyPart('user', length=5))


class RQ(Model):
    prefix = 'rq:'

    job = StringKeyPart('job')
    queue = StringKeyPart('queue')


class ProgressPercentage(Model):
    prefix = 'progress:'

    percentage = KeyScheme(IntKeyPart('user', length=5),
                           IntKeyPart('job', length=5))


if __name__ == "__main__":
    import redis

    db = redis.StrictRedis(host='127.0.0.1', port=6379, db=0)
    Model.db = db

    p = ProgressPercentage(53, user=123, job=456)
    print p
    print ProgressPercentage.percentage.get(user=123, job=456)

    print "GETTING", "DBID", id(db)
    p = ProgressPercentage(user=123, job=456)
    print "==", p()

    db2 = redis.StrictRedis(host='127.0.0.1', port=6379, db=0)
    print "GETTING", "DBID2", id(db2)
    p = ProgressPercentage(user=123, job=456, database=db2)
    print "==", p()

    print "GETTING", "DBID", id(db)
    p = ProgressPercentage(user=123, job=456, database=db)
    print "==", p()

    print "DONE"

    #
    # db2 = redis.StrictRedis(host='127.0.0.1', port=6379, db=0)
    i = RQ(val={'job': 'processing', 'queue': 123},
           job='AABBCC', queue='default',
           database=db2)
    print "==", i()
    print i.job._get_key(wildcard_ok=True)
    print i.job._get_key(wildcard_ok=False, job='AABBCC')
    print i.queue._get_key(wildcard_ok=True)

    for key, val in i.job.iter_items():
        print key, val

    RQ.job.set('finished', job='AABBCC')

    for key, val in i.job.iter_items():
        print key, val

    RQ.empty()

    print Progress.njobs.set(4, user=123)
    print Progress.njobs.get(user=123)
    for k in Progress.iter_keys():
        print 'iter', k

    p = ProgressPercentage(7, ex=1, user=123, job=456)
    print p
    import time

    while 1:
        v = ProgressPercentage(user=123, job=456)()
        time.sleep(0.1)
        if v is None:
            break
    print 'expired'

    print "CLEAN"
    for k in db.scan_iter('*'):
        print 'delete', k
        db.delete(k)
