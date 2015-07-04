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
import argparse
import re
import  itertools

# Constants
CHUNK_SIZE =        1024*1024*5
THREADS =           8
running =           True
verbose =           False

def debug(str):
    if(verbose):
        print str

def quit_program():
    global running
    running = False
    for t in sht:
        try:
            t.stop()
        except:
            pass
    for t in sht:
        try:
            t.join()
        except:
            pass
    sys.exit(-1)
    
def signal_handler(signal, frame):
    print('You pressed Ctrl+C!')
    quit_program()
    sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)

def scpfile(str):
    scpfile_regex = '^((([^:/?#@]+)@)?([^@/?#]*)?:)?(~?([^~#]*))$'
    try:
        match = re.match(scpfile_regex, str)
        obj = {}
        obj['path'] = match.group(5)
        obj['username'] = match.group(3)
        obj['host'] = match.group(4)
    except:
        msg = "%s does not appear to be a valid path" % str
        raise argparse.ArgumentTypeError(msg)
    if(obj['host'] and re.match('~.*', obj['path'])):
        msg = "expansion of '~' in '%s' currently unsupported" % obj['path']
        raise argparse.ArgumentTypeError(msg)
    return obj

parser = argparse.ArgumentParser()

parser.add_argument('-r', action='store_true', help="Recursively copy entire directories.")
parser.add_argument('src', metavar='[[user@]host1:]path', type=scpfile, nargs='+', help='')
parser.add_argument('-v', '--verbose', action='store_true', help="Enable verbose loggging")
args = parser.parse_args()

destination = args.src.pop()
source = args.src
if(args.verbose):
    verbose = True

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
        for srcdir in src:
            print "walking %s" % srcdir
            if(os.path.isdir(srcdir['path'])): #it's a direectory, walk it.
                for root, subFolders, files in os.walk(srcdir['path']):
                    print "files: %s, %s, %s" % (root, subFolders, files)
                    dest_dir = os.path.join(dest['path'], os.path.relpath(root, srcdir['path']))
                    #only keep the very end folder, cause were going to create dirs with parents.
                    if(not len(subFolders)):
                        self.folders.append("'" + dest_dir + "'")
                        print "dest: %s" % dest_dir
                    for file in files:
                        print(file)
                        filename = os.path.join(srcdir['path'], file) 
                        dest_file = os.path.join(dest_dir, file)
                        self.files.append(LocalFile(filename, dest_file))
            else: #its just a regular file
                print os.path.split(srcdir['path'])[1]
                dest_file = os.path.join(dest['path'], os.path.split(srcdir['path'])[1])
                print "copying %s to %s" % (srcdir['path'], dest_file)
                self.files.append(LocalFile(srcdir['path'], dest_file))
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
        print "copying file %s" % self.current_file['path']
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
        self.file_list = files
    def connect(self):
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh.connect(self.host, username=self.username, password=self.password)
    def run(self):
        self.connect()
        stdin, stdout, stderr = self.ssh.exec_command("which md5")
        if(len(stdout.readlines())):
            self.md5_cmd = "md5"
        else:
            self.md5_cmd = "md5sum"
        if(not running):
            return
        print "     " + current_thread().getName()
        self.running = True
        sftp = self.ssh.open_sftp()
        if(sftp is None):
            print "sftp is null"
        #iterate throught my file list
        for file in self.file_list.files:
            if(not running):
                return
            scpfile = None
            file.mutex.acquire()
            #try to get info about the file
            try:
                stat = sftp.stat(file.dest)
            except IOError:
                stat = None
            #if the file doesn't exist, or is the wrong size, create it.
            try:
                if(stat is None or stat.st_size != file.file_size):
                    if(stat is None):
                        scpfile = sftp.file(file.dest, 'w')
                    else:
                        scpfile = sftp.file(file.dest, 'w')
                    scpfile.seek(file.file_size-1)
                    scpfile.write('\0')
                else:
                    scpfile = sftp.file(file.dest, 'r+')
            except:
                print "ERROR: Could not open remote file."
                file.mutex.release()
                quit_program()
                return
            file.mutex.release()
            #write each chunk to the file.
            for chunk in file:
                if(not running):
                    scpfile.close()
                    return
                print "writing chunk %d of %s" % (chunk.offset, file.dest)
                #get the md5 of the remote chunk, just in case we have already transferred it.
                cmd = "dd if=%s bs=%d skip=%d count=1 | %s" %(file.dest, CHUNK_SIZE, chunk.offset, self.md5_cmd)
                stdin, stdout, stderr = self.ssh.exec_command(cmd)
                md5 = stdout.readline()[0:32]
                #if we haven't transferred it, do it until the md5 matches.
                while(md5 != chunk.md5):
                    if(not running):
                        scpfile.close()
                        return
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

# path = "/home/sam/Documents/mtscp/src"
# destination = "/home/sam/Documents/mtscp/dest"
file_list = LocalDir(source, destination)
    
sht = []
for i in range(0,THREADS):
    if(running):
        sht.append(SSH_Thread(destination['host'], destination['username'], '', file_list))
        if(i==0):
            debug("running mkdir on %s" % file_list.folders)
            sht[0].mkdir(file_list.folders)
        sht[i].start()
for t in sht:
    t.join()
print "all threads jioned"
sys.exit()
