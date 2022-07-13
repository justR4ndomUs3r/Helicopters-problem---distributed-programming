import numpy as np
import mpi4py
mpi4py.rc.initialize=False
import time
import threading
from mpi4py import MPI
import sys
from queue import Queue
import random

MPI.Init_thread(2)
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()
name = comm.Get_name()

lock = threading.Lock()
lock2 = threading.Lock()

H = size #helicopter
P = 4 #hangar
S = 2 #landing

status = 2
counter = 1

Que = Queue() #FIFO for messages

UnsendPermissionsLanding = [0]*size
UnsendPermissionsHangar = [0]*size
WrongPermissionLanding = [0]*size
WrongPermissionHangar = [0]*size

"""
Tags:
1 - ask for permission - landing
11 - ask for permission - landing with priority
2 - ask for permission - hangar
3 - permission for landing
4 - permission for hangar
5 - delete permission for landing
6 - delete permission for hangar

Helicopter Status:
0 - want to land
1 - want to start
2 - flying
3 - rest in hangar
5 - landing/starting
"""

def send(status, count):
    for i in range(size):
        if (i!=rank):
            if (status==0):
                req = comm.isend(count,dest=i, tag=1)
                req = comm.isend(count,dest=i, tag=2)
            if (status==1):
                req = comm.isend(count,dest=i, tag=11)
                

def odbieranie():
    while True:
        global WrongPermissionHangar, WrongPermissionLanding, counter, status, UnsendPermissionsLanding, UnsendPermissionsHangar
        message = 0
        state = MPI.Status()
        req = comm.irecv(source = MPI.ANY_SOURCE, tag = MPI.ANY_TAG)
        message = req.wait(status=state)
        tag = state.Get_tag()
        source = state.Get_source()
        if ((status == 0 or status == 2 or status == 3) and tag==11):
                req = comm.isend(message,dest=source, tag=3)
        elif ((status == 5) and (tag == 1 or tag == 11)):
            with lock:
                UnsendPermissionsLanding[source]=message
        elif ((status == 5) and (tag == 2)):
            with lock2:
                UnsendPermissionsHangar[source]=message
        elif((status == 2 or status == 3) and (tag == 1)):
            req = comm.isend(message,dest=source, tag=3)
            if(status == 3 or message>counter or (message == counter and rank > source)):
                WrongPermissionLanding[source] = message
        elif(status == 2 and tag == 2):
            req = comm.isend(message,dest=source, tag=4)
            if(message>counter or (message == counter and rank > source)):
                WrongPermissionHangar[source] = message
        else:
            tmp = [tag, source, message]
            Que.put(tmp)
    

def ladowanie(counter):
    global WrongPermissionHangar, WrongPermissionLanding, status
    odebraneLadowisko = 0
    odebraneHangar = 0
    for i in range(size):
        if(i!=rank):
            with lock:
                if(WrongPermissionLanding[i]>0):
                    mess = WrongPermissionLanding[i]
                    WrongPermissionLanding[i] = 0
                    UnsendPermissionsLanding[i] = mess
                    req = comm.isend(mess,dest=i, tag=5)
            if(WrongPermissionHangar[i]>0):
                with lock2:
                    mess = WrongPermissionHangar[i]
                    WrongPermissionHangar[i] = 0
                    UnsendPermissionsHangar[i] = mess
                    req = comm.isend(mess,dest=i, tag=6)
    send(status, counter)
    while ((odebraneLadowisko < (H-S) or odebraneHangar < (H-P) or not(Que.empty()))):
        if not(Que.empty()):
            dane = Que.get()
            tag = dane[0]
            source = dane[1]
            mess_count = dane[2]
            if (tag==1):
                if(mess_count<counter):
                    req = comm.isend(mess_count,dest=source, tag=3)
                elif (mess_count == counter and source>rank ):
                    req = comm.isend(mess_count,dest=source, tag=3)
                else:
                    with lock:
                        UnsendPermissionsLanding[source]=mess_count
            if (tag==2):
                if(mess_count<counter):
                    req = comm.isend(mess_count,dest=source, tag=4)
                elif (mess_count == counter and source>rank ):
                    req = comm.isend(mess_count,dest=source, tag=4)
                else:
                    with lock2:
                        UnsendPermissionsHangar[source]=mess_count
            if (tag==3):
                if(mess_count == counter):
                    odebraneLadowisko+=1
            if (tag==4):
                if(mess_count == counter):
                    odebraneHangar+=1
            if (tag==5):
                if(mess_count == counter):
                    odebraneLadowisko = odebraneLadowisko - 1
            if (tag==6):
                if(mess_count == counter):
                    odebraneHangar = odebraneHangar - 1
    status = 5
    print('Helikopter {} laduje zgody {}'.format(rank, odebraneLadowisko))
    sys.stdout.flush()
    time.sleep(2)
    print('Helikopter {} parkuje w hangarze zgody {}'.format(rank, odebraneHangar))
    sys.stdout.flush()
    for i in range(size):
        if(i!=rank):
            with lock:
                if (UnsendPermissionsLanding[i]>0):
                    comm.isend(UnsendPermissionsLanding[i],dest=i, tag=3)
                    WrongPermissionLanding[i]=UnsendPermissionsLanding[i]
                    UnsendPermissionsLanding[i]=0
    

def startowanie(counter):
    global WrongPermissionLanding, status, WrongPermissionHangar
    odebraneLadowisko = 0
    for i in range(size):
        if(i!=rank):
            with lock:
                if(WrongPermissionLanding[i]>0):
                    mess = WrongPermissionLanding[i]
                    WrongPermissionLanding[i] = 0
                    UnsendPermissionsLanding[i] = mess
                    req = comm.isend(mess,dest=i, tag=5)
    send(status, counter)
    while (odebraneLadowisko < (H-S) or not(Que.empty() )):
        if not(Que.empty()):
            dane = Que.get()
            tag = dane[0]
            source = dane[1]
            mess_count = dane[2]
            if (tag == 11):
                if(mess_count < counter):
                    req = comm.isend(mess_count,dest=source, tag=3)
                elif (mess_count == counter and source>rank):
                    req = comm.isend(mess_count,dest=source, tag=3)
                else:
                    with lock:
                        UnsendPermissionsLanding[source]=mess_count
            if (tag==1):
                with lock:
                    UnsendPermissionsLanding[source]=mess_count
            if (tag==2):
                with lock2:
                    UnsendPermissionsHangar[source]=mess_count
            if (tag==3):
                if(mess_count == counter):
                    odebraneLadowisko+=1
            if (tag==5):
                if(mess_count == counter):
                    odebraneLadowisko = odebraneLadowisko - 1
    status = 5
    print('Helikopter {} startuje zgody {}'.format(rank, odebraneLadowisko))
    sys.stdout.flush()
    time.sleep(2)
    print('Helikopter {} wystartowal'.format(rank))
    sys.stdout.flush()
    for i in range(size):
        if(i!=rank):
            with lock:
                if (UnsendPermissionsLanding[i]>0):
                    count_test = counter+1
                    if ( UnsendPermissionsLanding[i] > count_test or (UnsendPermissionsLanding[i] == count_test and i < rank)):
                        WrongPermissionLanding[i] = UnsendPermissionsLanding[i]
                    comm.isend(UnsendPermissionsLanding[i],dest=i, tag=3)
                    UnsendPermissionsLanding[i]=0
            with lock2:
                if (UnsendPermissionsHangar[i] > 0):
                    count_test = counter+1
                    if ( UnsendPermissionsHangar[i] > count_test or (UnsendPermissionsHangar[i] == count_test and i < rank)):
                        WrongPermissionHangar[i] = UnsendPermissionsHangar[i]
                    comm.isend(UnsendPermissionsHangar[i],dest=i, tag=4)
                    UnsendPermissionsHangar[i]=0


def run():
    while(True):
        global status, counter, WrongPermissionHangar, WrongPermissionLanding
        status = 2
        print("Helikopter {} lata".format(rank))
        sys.stdout.flush()
        time.sleep(random.randint(1,10))
        status = 0
        print("Helikopter {} chce wyladowac".format(rank))
        sys.stdout.flush()
        ladowanie(counter)
        counter+=1
        status = 3
        print("Helikopter {} odpoczywa w hangarze".format(rank))
        sys.stdout.flush()
        time.sleep(random.randint(1,10))
        status = 1
        print("Helikopter {} chce wystartowac".format(rank))
        sys.stdout.flush()
        startowanie(counter)
        counter+=1
        

def main():
    thread1 = threading.Thread(target=run)
    thread2 = threading.Thread(target=odbieranie)
    thread1.start()
    thread2.start()

main()
