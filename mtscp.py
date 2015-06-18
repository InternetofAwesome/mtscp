#!/usr/bin/python

from threading import Thread, Lock
from multiprocessing import Pool
import Queue
import paramiko
import signal
import sys
import time  # remove later
import os
import md5
import math
from urlparse import urlparse
from IPython.external.ssh.tunnel import paramiko
from guidata.dataset import datatypes
from hgext.mq import queue
from hgext.keyword import files
from mercurial.demandimport import nothing
from mutex import mutex
# from Crypto.SelfTest.Random.test__UserFriendlyRNG import multiprocessing

# Constants
CHUNK_SIZE =     1024*64
THREADS =        8
host = '127.0.0.1'
username = 'sam'
password = ''

def signal_handler(signal, frame):
    print('You pressed Ctrl+C!')
    sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)

class Chunk:
    def __init__(self):
        self.data=""
        self.md5=""
        self.path=""
        self.dest=""
        self.chunk_size=0
        self.offset=0

class LocalFile:
    def __init__(self, filename, dest, file_size=None):
        print "constructor"
        self.filename = filename
        self.dest = dest
        self.mutex = Lock()
        self.file_size = file_size
    def open(self):
        self.file = open(self.filename, "ab+")
    def __iter__(self):
        return self
    def next(self):
        chunk = Chunk()
        self.mutex.acquire()
        chunk.offset = self.file.tell()/CHUNK_SIZE
        chunk.size = -self.file.tell()
        chunk.data = self.file.read(CHUNK_SIZE)
        chunk.size += self.file.tell()
        self.mutex.release()
        chunk.dest = self.dest
        chunk.md5 = md5.new()
        chunk.md5.update(chunk.data)
        chunk.md5 = str(bytearray(chunk.md5.digest())).encode('hex')
#         print "created chunk #%d" % chunk.offset
        return chunk
    def write(self, offset, data):
        self.mutex.acquire()
        self.file.seek(offset)
        self.file.write(data)
        self.mutex.release()
    def md5(self, start, size):
        m = md5.new()
        self.mutex.acquire()
        self.file.seek(start)
        m.update(self.file.read(size))
        self.mutex.release()
        return m.digest()
        
   
class SSH_Thread(Thread):
    mutex = Lock()
    files = []
    current_file = None
    def __init__(self, mode, host, username, password):
        Thread.__init__(self)
        print "SSH_Thread constructor"
        self.mode = mode
        self.host = host
        self.username = username
        self.password = password
        self.file_size = None
        self.file = None
        self.size=0
        self.time=0
        self.running = False
        self.kill = False
        self.connect()
        stdin, stdout, stderr = self.ssh.exec_command("which md5")
#         print len(stdout.readline())
        if(len(stdout.readlines())):
            self.md5_cmd = "md5"
        else:
            self.md5_cmd = "md5sum"
    def connect(self):
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh.connect(host, username=username, password=password)
    def run(self):
        while(1):
            SSH_Thread.mutex.acquire()
            if(SSH_Thread.current_file is None):
                SSH_Thread.current_file = SSH_Thread.files.pop()
                SSH_Thread.current_file.open()
            SSH_Thread.mutex.release()
            for chunk in SSH_Thread.current_file:
                self.chunk_write(chunk)
    def stop(self):
        self.running = False
    def kill(self):
        self.kill = True
    def chunk_write(self, chunk):
        ssh_md5=""
        
        while(chunk.md5 != ssh_md5):
            try:
                print `chunk.offset`
    #             if(chunk.size == CHUNK_SIZE):
                cmd = "dd of='%s' count=1 bs=%d seek=%d conv=notrunc && dd if='%s' count=1 bs=%d skip=%d | md5sum" % (chunk.dest, CHUNK_SIZE, chunk.offset, chunk.dest, CHUNK_SIZE, chunk.offset)
    #             else:
    #                 cmd = "dd of='%s' bs=%d conv=notrunc seek=%d" % (chunk.dest, CHUNK_SIZE, chunk.offset) 
    #             print cmd
                stdin, stdout, stderr = self.ssh.exec_command(cmd)
    #             stdin.channel.send(chunk.data)
                stdin.write(chunk.data)
                stdin.channel.shutdown_write()
                ssh_md5 = stdout.readline()[0:32]
    #             pridnt "md5: '" + ssh_md5 + "' == '" + chunk.md5 + "' " +  `(chunk.md5 == ssh_md5)` + " " + `len(chunk.data)`
            except:
                print "some bad shit happened."
                self.connect()
   
print "dickbath"
path = "/home/sam/Downloads/Dropbox"
destination = "/home/sam/trash"
for root, subFolders, files in os.walk(path):
    for file in files:
        filename = os.path.join(path, file) 
        print "filename: " + filename
        dest = destination + "/" + os.path.relpath(filename, path)
        print "dest: " + dest
        SSH_Thread.files.append(LocalFile(filename, dest))
for p in SSH_Thread.files:
    print p.filename
    
sht = []
for i in range(0,THREADS):
    sht.append(SSH_Thread('w', '127.0.0.1', 'sam', ''))
    sht[i].start()    
sys.exit()
    
# queue = Queue(THREADS+1)
if __name__ == "__main__":
    cg = Chunk_Generator("/home/sam/Downloads/Dropbox", "/home/sam/trash")
#     cg.queue = queue  
    cg.start()
#     while(cg.queue.get() or cg.is_alive()):   
#         pass 
#     sys.exit()

    for i in range(0,THREADS):
        sht[i].stop()
    for i in range(0,THREADS):
        sht[i].join()
    cg.join()
    print "would exit"
    # sys.exit()


# f = SSHFile('freenas.local', 'root', '')
# chunk = Chunk()
# chunk.data = "Dickbath fartnose"
# md5 = md5.new()
# md5.update(chunk.data)
# chunk.md5 = md5.digest()
# chunk.md5 = str(bytearray(chunk.md5)).encode('hex')
# chunk.chunk_size = len(chunk.data)
# chunk.path  = "/mnt/main/media/trash"
# chunk.offset = 0
# print "calculated source md5: '" + chunk.md5 + "'"
# f.chunk_write(chunk)


 
# for thread in t: 
#     thread.join()
# 
# ssh = paramiko.SSHClient()
# ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
# try:
#     print "creating connection"
#     ssh.connect(host, username=username, password=password)
#     print "connected"
#     stdin, stdout, stderr = ssh.exec_command("dd of=~/test count=1024")
#     print "writing"
#     stdin.write("Testing some shit\r\n")
#     stdin.write("Other shit\r\n")
#     stdin.channel.shutdown_write();
#     stdin.flush()
#     print "reading"
#     # if stderr.channel.recv_stderr_ready():
#     print stderr.readlines()	
#     stdin, stdout, stderr = ssh.exec_command("md5 ~/test")
#     print stdout.readlines()
# except KeyboardInterrupt:
#     print "Bye"
#     sys.exit()
# finally:
#     print "closing connection"
#     ssh.close()
#     print "closed"
