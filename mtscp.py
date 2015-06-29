#!/usr/bin/python

import threading
from threading import Thread, Lock, current_thread, Semaphore
import Queue
import paramiko
import signal
import sys
import time  # remove later
import os
import md5
import math
from urlparse import urlparse
import paramiko
from optparse import OptionParser

# Constants
CHUNK_SIZE =     1024*1024
THREADS =       8

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
class File:
    def __init__(self, src, dest):
        self.mutex = Lock()
        self.src = src
        self.dest = dest
        self.mutex = Lock()
class LocalFile(File):
    def __init__(self, src, dest):
        File.__init__(self, src, dest)
        self.dest_created = False
        self.open()
    def open(self):
        self.file = open(self.src, "ab+")
        self.file.seek(0, 2)
        self.file_size = self.file.tell();
        self.file.seek(0)
        while(self.file.tell() !=0):
            print "waiting on seek"
    def __iter__(self):
        return self
    def next(self):
        chunk = Chunk()
        self.mutex.acquire()
        if not self.file:
            self.mutex.release()
            raise StopIteration()
        chunk.offset = self.file.tell()/CHUNK_SIZE
        chunk.size = -self.file.tell()
        chunk.data = self.file.read(CHUNK_SIZE)
        if not chunk.data:
            self.mutex.release()
            raise StopIteration()
        chunk.size += self.file.tell()
        self.mutex.release()
        chunk.dest = self.dest
        chunk.md5 = md5.new()
        chunk.md5.update(chunk.data)
        chunk.md5 = str(bytearray(chunk.md5.digest())).encode('hex')
        if(chunk.size != CHUNK_SIZE):
            print "read chunk %d, size: %d" % (chunk.offset, chunk.size)
            sys.exit()
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
    def eof(self):
        self.mutex.acquire()
        ret = self.file_size == self.file.tell()
        self.mutex.release()
        return ret
#     def dest_created(self, created=None):
#         ret = False
#         self.mutex.acquire()
#         if(created is None):
#             ret = self.dest_created
#         else
#             self.dest_created = CREATED
#             ret = True
#         self.mutex.release()
#         return ret
    
class LocalDir:
    def __init__(self, src, dest):
        self.files = []
        self.folders = []
        self.semaphore = Semaphore(THREADS)
        self.current_file = None
        for root, subFolders, files in os.walk(src):
            print "%s, %s, %s" % (root, subFolders, files)
            dest_dir = os.path.join(dest, os.path.relpath(root, src))
            #only keep the very end folder, cause were going to create dirs with parents.
            if(not len(subFolders)):
                self.folders.append("'" + dest_dir + "'")
                print "dest: %s" % dest_dir
            for file in files:
                print(file)
                filename = os.path.join(src, file) 
                dest_file = os.path.join(dest_dir, file)
                self.files.append(LocalFile(filename, dest_file)) 
        #note that this totally ignores maximum command line length (which is huge)
        self.folders = " ".join(self.folders)
        print "Folders: %s" % self.folders
    def __iter__(self):
        return self
    def next(self):
        if(self.current_file is None or self.current_file.eof()):
            if(len(self.files)):
                self.current_file = self.files.pop()
            else:
                raise StopIteration()
        return self.current_file
class SSH_Thread(Thread):
    mutex = Lock()
    files = []
    current_file = None
    def __init__(self, host, username, password, files):
        Thread.__init__(self)
        print "SSH_Thread constructor"
        self.host = host
        self.username = username
        self.password = password
        self.size=0 
        self.time=0
        self.running = False
        self.kill = False
        self.file_list = files
        self.connect()
        stdin, stdout, stderr = self.ssh.exec_command("which md5")
        if(len(stdout.readlines())):
            self.md5_cmd = "md5"
        else:
            self.md5_cmd = "md5sum"
    def connect(self):
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh.connect(host, username=username, password=password)
    def run(self):
        print "starting thread " + current_thread().getName()
        self.running = True
        sftp = self.ssh.open_sftp()
        if(sftp is None):
            print "sftp is null"
        #iterate throught my file list
        for file in self.file_list:
            scpfile = None
            file.mutex.acquire()
            print "looking at file %s" % file.dest
            #try to get info about the file
            try:
                stat = sftp.stat(file.dest)
            except IOError:
                stat = None
            #if the file doesn't exist, or is the wrong size, create it.
            if(stat is None or stat.st_size != file.file_size):
                if(stat is None):
                    print file.dest
                    scpfile = sftp.file(file.dest, 'w+')
                else:
                    scpfile = sftp.file(file.dest, 'r+')
                scpfile.seek(file.file_size-1)
                scpfile.write('\0')
            else:
                scpfile = sftp.file(file.dest, 'r+')
            file.mutex.release()
            #write each chunk to the file.
            for chunk in file:
                print "writing chunk %d of %s" % (chunk.offset, file.dest)
                #get the md5 of the remote chunk, just in case we have already transferred it.
                cmd = "dd if=%s bs=%d skip=%d count=1 | %s" %(file.dest, CHUNK_SIZE, chunk.offset, self.md5_cmd)
                stdin, stdout, stderr = self.ssh.exec_command(cmd)
                md5 = stdout.readline()[0:32]
                #if we haven't transferred it, do it until the md5 matches.
                while(md5 != chunk.md5):
                    print "trying chunk %d in %s" % (chunk.offset, current_thread().getName())
                    scpfile.seek(chunk.offset*CHUNK_SIZE)
                    scpfile.write(chunk.data)
                    print cmd
                    stdin, stdout, stderr = self.ssh.exec_command(cmd)
                    md5 = stdout.readline()[0:32]
                    print "chunk.md5: %s, md5: %s" %(chunk.md5, md5)
            scpfile.close();
            print "leaving thread " + current_thread().getName()
    def stop(self):
        self.running = False
    def kill(self):
        self.kill = True
    def mkdir(self, folder):
        cmd = "mkdir -p %s" % folder
        print cmd
        stdin, stdout, stderr = self.ssh.exec_command(cmd)
        if(stdout.channel.recv_exit_status()):
            print "stub: failed to create remote dir"
            return 1
    def chunk_write(self, chunk):
        ssh_md5=""
        
        while(chunk.md5 != ssh_md5):
            try:
                print "writing %d bytes - %d" % (len(chunk.data), chunk.offset)
                file = self.ssh.open_sftp().file(chunk.dest, 'r+')
                file.seek(CHUNK_SIZE*chunk.offset)
                file.write(chunk.data)
                file.close();
                cmd = "dd if='%s' count=1 bs=%d skip=%d | md5sum" % (chunk.dest, CHUNK_SIZE, chunk.offset)
                stdin, stdout, stderr = self.ssh.exec_command(cmd)
                ssh_md5 = stdout.readline()[0:32]
                print "md5: '" + ssh_md5 + "' == '" + chunk.md5 + "' " +  `(chunk.md5 == ssh_md5)` + " " + `len(chunk.data)`
                print stderr.readlines();
            except:
                print "some bad shit happened."
                self.connect()

path = "/home/sam/Documents/mtscp/src"
destination = "/home/sam/Documents/mtscp/dest"

file_list = LocalDir(path, destination)
    
sht = []
for i in range(0,THREADS):
    sht.append(SSH_Thread('localhost', 'sam', '', file_list))
    if(i==0 ):
        print "running mkdir on %d" % i
        sht[0].mkdir(file_list.folders)
    sht[i].start()
for t in sht:
    t.stop()
    t.join()
print "all threads jioned"
sys.exit()
