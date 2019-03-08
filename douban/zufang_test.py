import multiprocessing
import time



def print__num(num,Q):
    for i in range(num):
        # print(num)
        Q.put(i)
        time.sleep(1)

def getNum(Q):
    while not Q.qsize()>1:
        qnum = Q.get(block=False)
        print(qnum)



if __name__ == '__main__':
    q_num = multiprocessing.Queue()
    print(q_num.empty())
    p1 = multiprocessing.Process(target=print__num,args=(10,q_num))
    p2 = multiprocessing.Process(target=print__num,args=(5,q_num))
    p3 = multiprocessing.Process(target=getNum,args=(q_num,))
    p1.start()
    p2.start()
    p3.start()
    p1.join()
    p2.join()
    p3.join()
