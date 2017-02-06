#!/usr/bin/python

import random
import multiprocessing
from mysqldb import Mysql


def test_for_update():
    mysqldb = Mysql('root', '123456', 'test')
    success, msg = mysqldb.updateMutexCheck('user', {"uid": 4}, {"status": 27}, {"status": [22, 26, 28, 40]})
    if not success:
        print msg
    mysqldb.disconnect()

if __name__ == "__main__":
    mysqldb = Mysql('root', '123456', 'test')
    #print mysqldb.get_one('user', {})
    #print mysqldb.update('user', {"status": 1}, {"nickname": "123", "sex": "f"})
    #print mysqldb.updateMutexCheck('user', {"uid": 4}, {"status": 20}, {"status": {"!=": 20}})
    #start = 5
    #s = 'abcdefghijklmnopqrstuvwxyz'
    #t = time.time()
    #datas = []
    #for i in range(10):
    #    _id = start + 1
    #    length = random.randrange(1, 20)
    #    nickname = []
    #    for i in range(length):
    #        nickname.append(random.choice(s))
    #    nickname = ''.join(nickname)
    #    data = {"uid": _id, "sex": random.choice(['m', 'f']), "nickname": nickname, 'status': random.randrange(0, 30)}
    #    print mysqldb.insert('user', data)
    #    datas.append(data)
    #    start += 1
    #print datas
    #mysqldb.insert_batch('user', datas)
    #end = time.time()
    #print end - t
    ps = []
    for i in range(30):
        p = multiprocessing.Process(target=test_for_update)
        p.start()
        ps.append(p)
    for i in ps:
        i.join()
    #
    mysqldb.disconnect()
