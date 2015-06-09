#!/usr/bin/python

from threading import Thread, Lock
import paramiko
import signal
import sys
import time  # remove later
import os
import md5
from urlparse import urlparse

# Constants
CHUNK_SIZE=1024

# md5sum ./mtscp | sed 's/.*\([0-9a-fA-F]\{32\}\).*/\1/'

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
    
    
class SCPWriter(Thread):
    def __init__(self, host, port, user, passwd, queue):
        Thread.__init__(self)
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    def run(self):
        self.ssh.connect(host, username=user, password=passwd)
        while(!done):
            try:
                item = self.queue.get_nowait(True)
            except Empty:
                continue
            break
            md5_data = md5.new()
            md5_data.update(item.data)
            md5_data.digest()
            md5_data = str(bytearray(md5.digest())).encode('hex')
            md5_remote = ""
            
            while(md5_remote != md5_data):
                stdin, stdout, stderr = self.ssh.exec_command("dd of=%s bs=%d seek=%d count=1" \
                  % (item.filename, len(item.data), item.chunk_offset))
                stdin.write(item.data)
                stdin.flush()
                stdin.channel.shutdown_write();
                stdin, stdout, stderr = self.ssh.exec_command("dd if=%s bs=%d seek=%d count=1 | md5sum" \
                  % (item.filename, len(item.data), item.chunk_offset))
                md5_remote = stdout.read_lines()
                md5_remote = md5.pop()[0::32]
    def finish(self):
        self.done=True
    def exec_command(self, command):
        self.stdin, self.stdout, self.stderr = self.ssh.exec_command(command)
    def get_stdout(self):
        
 

   
class SSHFile:
    max_threads=8
    def __init__(self):
        print "constructor"
        self.mutex = Lock();
        self.file_size = None
        self.file = None
        self.queue = Queue(max_threads)
    def open(self, filename, file_size=None):
        self.url = urlparse(filename)
        print "username: " + o.username
        print "host: " + o.hostname
        print "path: " + o.path 
        ssh
        
        
        
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
    

f = LocalFile()
f.open("/home/sam/rand")
print f.md5_string(0, f.size())

t = []
for i in range(0, 5):
    t.append(SSHThread('sam@192.168.10.112', '22', '', 'asdf'))
    t[i].start()
 
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
