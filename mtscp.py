#!/usr/bin/python

from threading import Thread
from multiprocessing import Process
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

# Constants
CHUNK_SIZE =     1024*8
THREADS =        8
host = '192.168.10.112'
username = 'sam'
password = ''

def signal_handler(signal, frame):
    print('You pressed Ctrl+C!')
    sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)

class LocalFile:
    def __init__(self):
        print "constructor"
        self.mutex = Lock();
        self.file_size = None
        self.file = None
    def open(self, filename, file_size=None):
        self.file = open(filename, "ab+")
        self.file.seek(0, 2)
        self.file_size = self.file.tell()
        print "computed file size is " + `self.file_size`
        if file_size is not None:
            if(file_size > self.file_size):
                self.file.seek(file_size-1)
                self.file.write("\0")
            elif(file_size < self.file_size):
                self.file.truncate(file_size)
            self.file_size = file_size    
        if(self.file.closed):
            print "File is closed, dummy"
    def read(self, offset, size):
        self.mutex.acquire()
        self.file.seek(offset)
        data = self.file.read(size)
        self.mutex.release()
        return data
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
    def md5_string(self, start, size):
        nums = self.md5(start, size)
        return str(bytearray(nums)).encode('hex')
    def size(self):
        return self.file_size
    
    
# class SCPWriter(Thread):
#     def __init__(self, host, port, user, passwd, queue):
#         Thread.__init__(self)
#         self.ssh = paramiko.SSHClient()
#         self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#     def run(self):
#         self.ssh.connect(host, username=user, password=passwd)
#         while(!done):
#             try:
#                 item = self.queue.get_nowait(True)
#             except Empty:
#                 continue
#             break
#             md5_data = md5.new()
#             md5_data.update(item.data)
#             md5_data.digest()
#             md5_data = str(bytearray(md5.digest())).encode('hex')
#             md5_remote = ""
#             
#             while(md5_remote != md5_data):
#                 stdin, stdout, stderr = self.ssh.exec_command("dd of=%s bs=%d seek=%d count=1" \
#                   % (item.filename, len(item.data), item.chunk_offset))
#                 stdin.write(item.data)
#                 stdin.flush()
#                 stdin.channel.shutdown_write();
#                 stdin, stdout, stderr = self.ssh.exec_command("dd if=%s bs=%d seek=%d count=1 | md5sum" \
#                   % (item.filename, len(item.data), item.chunk_offset))
#                 md5_remote = stdout.read_lines()
#                 md5_remote = md5.pop()[0::32]
#     def finish(self):
#         self.done=True
#     def exec_command(self, command):
#         self.stdin, self.stdout, self.stderr = self.ssh.exec_command(command)
#     def get_stdout(self):

class Chunk:
    def __init__(self):
        self.data=""
        self.md5=""
        self.path=""
        self.dest=""
        self.chunk_size=0
        self.offset=0

class Chunk_Generator(Process):
    def __init__(self, path, dest): 
        Process.__init__(self)
        self.files=[]
        self.folders=[]
        self.path = path
        self.dest = dest
        self.queue = Queue.Queue(THREADS+1)
    def run(self):
        for root, subFolders, files in os.walk(self.path):
            for file in files:
                filename = os.path.join(root, file) 
                dest = self.dest + "/" + os.path.relpath(filename, self.path)
                print "dest: " + dest
                try:
                    fhnd = open(filename, 'r')
                    fhnd.seek(0,2)
                    size = fhnd.tell()
                    chunks = int(math.ceil(float(size)/CHUNK_SIZE)) + 1
                    print('getting %d chunks' % chunks)
                    
                    for i in range(0, chunks):
#                         print("chunk %d" %  i)
                        elapsed = -time.time()
                        chunk = Chunk()
                        chunk.path = filename
                        chunk.dest = dest
                        chunk.offset = i
                        fhnd.seek(i*CHUNK_SIZE)
                        chunk.data = fhnd.read(CHUNK_SIZE)
                        chunk.size = fhnd.tell()-i*CHUNK_SIZE
#                         print chunk.size
                        chunk.md5 = md5.md5()
                        chunk.md5.update(chunk.data)
                        chunk.md5 = str(bytearray(chunk.md5.digest())).encode('hex')
#                         print chunk.path + " - " + `chunk.offset` + " - " + chunk.md5
#                         elapsed -= time.time()
                        self.queue.put(chunk)
                        elapsed += time.time()
                        print "chunk read speed: " + `chunk.size/elapsed`
                        
                except:
                    print "fucked up"
                    pass
            
 
class File_Thread:
    def __init__(self, mode, queue):
        self.running = False
        self.kill = False
        self.mode = mode
        self.incoming = queue
        self.outgoing = Queue(THREADS*2)
    def run(self):
        if(self.mode == 'w'):
            while(self.running or not self.incoming.empty() and not self.kill):
                print "dickbath"
                
        
   
class SSH_Thread(Process):
    def __init__(self, mode, queue, host, username, password):
        Process.__init__(self)
        print "SSH_Thread constructor"
        self.mode = mode
        self.file_size = None
        self.file = None
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh.connect(host, username=username, password=password)
        self.size=0
        self.time=0
        self.running = False
        self.kill = False
        self.queue = queue
        stdin, stdout, stderr = self.ssh.exec_command("which md5")
#         print len(stdout.readline())
        if(len(stdout.readlines())):
            self.md5_cmd = "md5"
        else:
            self.md5_cmd = "md5sum"
    def run(self):
        if(self.mode == 'w'):
            self.running = True
            while(self.running or not self.queue.empty() and not self.kill):
                self.time -= time.time()
                try:
                    chunk = self.queue.get(True, 0.2)
#                     print("doing chunk %d" % chunk.offset)
                    self.size += chunk.size
                    self.chunk_write(chunk)
                    self.time += time.time()
#                     print "speed: " + `self.size/self.time*1000`
                except Queue.Empty:
                    print("empty queue")
                    pass
                
                
                
    def stop(self):
        self.running = False
    def kill(self):
        self.kill = True
    def chunk_read(self, path, offset, chunk_size):
        print "chunk read"

    def chunk_write(self, chunk):
        ssh_md5=""
        while(chunk.md5 != ssh_md5):
            print "writing chunk %d" % chunk.offset
            if(chunk.size == CHUNK_SIZE):
                cmd = "dd of='%s' count=1 bs=%d seek=%d conv=notrunc" % (chunk.dest, CHUNK_SIZE, chunk.offset)
            else:
                cmd = "dd of='%s' bs=%d conv=notrunc seek=%d" % (chunk.dest, CHUNK_SIZE, chunk.offset) 
#             print cmd
            stdin, stdout, stderr = self.ssh.exec_command(cmd)
#             stdin.channel.send(chunk.data)
            stdin.write(chunk.data)
            stdin.flush()
            stdin.channel.shutdown_write()
            
#             lines = stderr.readlines()
#             print lines[2]
#             Calculate the md5 of the file chunk that was written
            cmd="dd if='%s' count=1 bs=%d skip=%d | %s" % (chunk.dest, CHUNK_SIZE, chunk.offset, self.md5_cmd) 
#             print cmd
            stdin, stdout, stderr = self.ssh.exec_command(cmd)
            ssh_md5 = stdout.readline().strip()
#             print "md5: '" + ssh_md5 + "' == '" + chunk.md5 + "' " +  `(chunk.md5 == ssh_md5)` + " " + `len(chunk.data)`
    
queue = Queue.Queue(THREADS+1)
if __name__ == "__main__":
    cg = Chunk_Generator("/home/sam/Downloads/Dropbox", "/mnt/main/media/trash")
    cg.queue = queue  
    cg.start()
    # while(cg.queue.get() or cg.is_alive()):   
    #     pass 
    # sys.exit()
    sht = []
    for i in range(0,THREADS):
        sht.append(SSH_Thread('w', cg.queue, 'freenas.local', 'root', ''))
        sht[i].start()
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
