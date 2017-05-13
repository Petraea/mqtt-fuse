#!/usr/bin/env python
import os, sys, errno, time
import pwd, grp, resource
import StringIO

from fuse import FUSE, FuseOSError, Operations
from multiprocessing import Process, Manager
import paho.mqtt.client as mqtt

def log(f):
    '''Decorator function for debugging.'''
    def my_f(*args, **kwargs):
        print (f.__name__)
        print (args)
        return f(*args, **kwargs)
    return my_f


def getTree(path):
    currpath=tree
    for p in path:
        currpath=currpath[0][p]
    return currpath

def putTree(path,payload):
    tpath=[('',tree[:])]
    for n,p in enumerate(path):
        if p not in tpath[-1][1][0] and isinstance(tpath[-1][1][0],dict):
            tpath[-1][1][0][p]=[{},time.time(),time.time()]
        else:
            tpath[-1][1][2]=time.time()
        if n == len(path)-1:
            t=[None,None,None]
            t[0]=payload
            t[1]=time.time()
            t[2]=time.time()
            tpath[-1][1][0][p]=t
        else:
            tpath.append((p,tpath[-1][1][0][p]))
    while len(tpath)>=2:
        tpath[-2][1][0][tpath[-1][0]]=tpath[-1][1]
        del tpath[-1]
    tree[0] = tpath[0][1][0]
    tree[1] = tpath[0][1][1]
    tree[2] = tpath[0][1][2]


class MQTTClient():
    def __init__(self,tree,host,port=1883):
        self.tree = tree
        self.mqtt = mqtt.Client()
        self.mqtt.on_connect=self.on_connect
        self.mqtt.on_message=self.on_message
        self.mqtt.connect(host,port,keepalive=10)
        self.mqtt.loop_forever() #BLOCKING

    def on_connect(self, client, userdata, flags, rc):
        client.subscribe("#")

    def on_message(self, client, userdata, msg):
        parts = filter(bool,msg.topic.split('/'))
        putTree(parts,msg.payload)

class MQTTFS(Operations):
    def __init__(self,tree,host,port=1883):
        self.tree = tree
        self.mqtt = mqtt.Client()
        self.mqtt.connect(host,port,keepalive=10)
        self.filehandles={}
        self.fhmax=0

    def _fixpath(self, path):
        return filter(bool,path.split(os.sep))

    def access(self, path, mode):
        return True

    def chmod(self, path, mode):
        raise FuseOSError(errno.EPERM)

    def chown(self, path, uid, gid):
        raise FuseOSError(errno.EPERM)

    @log
    def getattr(self, path, fh=None):
        data={}
        path = self._fixpath(path)
        try:
            treedata=getTree(path)
        except:
            raise FuseOSError(errno.ENOENT)
        data['st_atime']=int(treedata[2])
        data['st_mtime']=int(treedata[2])
        data['st_ctime']=int(treedata[1])
        data['st_gid']=os.getgid()
        mode = int('777',8)
        if isinstance(treedata[0],dict):
            data['st_mode']=mode + 16384
            data['st_size']=4
        else:
            data['st_mode']=mode + 32768
            data['st_size']=len(treedata[0])
        data['st_nlink']=0
        data['st_uid']=os.getuid()
        return data

    @log
    def readdir(self, path, fh):
        '''Return all objects in this path'''
        path = self._fixpath(path)
        try:
            treedata=getTree(path)
        except:
            raise FuseOSError(errno.ENOENT)
        if isinstance(treedata[0],dict):
            return ['.','..']+treedata[0].keys()

    def readlink(self, path):
        '''No symlinks in mqtt.'''
        return path

    def mknod(self, path, mode, dev):
        '''No special devices in mqtt.'''
        raise FuseOSError(errno.EACCES)

    @log
    def rmdir(self, path):
        '''remove dir if empty. This shouldn't be needed.'''
        raise FuseOSError(errno.EACCES)

    @log
    def mkdir(self, path, mode):
        '''Make a new directory.'''
        path = self._fixpath(path)
        try:
            treedata=getTree(path[:-1])
        except:
            raise FuseOSError(errno.ENOENT)
        if path[-1] in treedata[0]:
            raise FuseOSError(errno.EEXIST)
        if isinstance(treedata[0],dict):
            putTree(path,{})
        else:
            raise FuseOSError(errno.ENOTDIR)

    @log
    def rename(self, old, new):
        '''Rename a file/directory. This probably shouldn't work...'''
        raise FuseOSError(errno.EACCES)

    @log
    def statfs(self, path):
        '''Statfs is used by things like df to get the current filesystem size.'''
        data={}
        data['f_bsize']=8
        data['f_blocks']=100
        data['f_bavail']=100
        data['f_bfree']=data['f_bavail']
        data['f_favail']=data['f_bavail']
        data['f_ffree']=data['f_bavail']
        data['f_files']=data['f_blocks']
        data['f_flag']=os.O_DSYNC
        data['f_frsize']=data['f_bsize']
        data['f_namemax']=255
        return data

    def unlink(self, path):
        '''AKA rm. You aren't allowed to unpublish data.'''
        raise FuseOSError(errno.EACCES)

    def symlink(self, name, target):
        '''Symlinks and hardlinks don't work.'''
        raise FuseOSError(errno.EACCES)

    def link(self, target, name):
        '''Symlinks and hardlinks don't work.'''
        raise FuseOSError(errno.EACCES)

    @log
    def utimens(self, path, times=None):
        '''AKA touch. Update times on path.'''
        path = self._fixpath(path)
        try:
            treedata=getTree(path)
        except:
            raise FuseOSError(errno.ENOENT)
        if times is None:
            treedata[2]=time.time()
        else:
            treedata[2]=times[1] #(atime,mtime)

    def open(self, path, flags):
        '''Open a filehandle as an IObuffer.'''
        return self.create(path,flags)

    def create(self, path, mode, fi=None):
        '''Open a nonexistent file. This will just create a new file and generate a
        new filehandle for you.'''
        maxfh = resource.getrlimit(resource.RLIMIT_NOFILE)[0]
        path = self._fixpath(path)
        try:
            treedata=getTree(path[:-1])
        except:
            raise FuseOSError(errno.ENOENT)
        if not isinstance(treedata[0],dict):
            raise FuseOSError(error,ENOENT)
        if path[-1] not in treedata[0]:
            putTree(path,'')
            treedata=getTree(path[:-1])
        if len(self.filehandles) == maxfh:
            raise FuseOSError(errno.EMFILE)
        while self.fhmax in self.filehandles.keys():
            self.fhmax = (self.fhmax+1)%maxfh
        self.filehandles[self.fhmax] = StringIO.StringIO()
        self.filehandles[self.fhmax].write(treedata[0][path[-1]][0])
        return self.fhmax

    @log
    def read(self, path, length, offset, fh):
        try:
            treedata=getTree(path)
            return treedata[0]
        except:
            f = self.filehandles[fh]
            f.seek(offset)
            return f.read(length)

    @log
    def write(self, path, buf, offset, fh):
        '''Write to the object, and publish here.'''
        try:
            data = getTree(path)[0][offset:].split()
        except:
            data = ['']*offset
        f = self.filehandles[fh]
        newdata=['']*offset+list((f.read()+buf).strip())
        for n, c in enumerate(data):
            if newdata[n] == '':    
                newdata[n]=c
        print(newdata)
        pubdata = ''.join(newdata)
        if pubdata != '':
            if offset == 0: #Hack here. 
                print(path[1:], pubdata)
                try: pubdata = float(pubdata)
                except: pass
                self.mqtt.publish(path[1:],pubdata,qos=1)
        return True

    @log
    def truncate(self, path, length, fh=None):
        if fh is None:
            fh = self.fhmax
        self.filehandles[fh].truncate(length)

    @log
    def flush(self, path, fh):
        '''Empty the buffer.'''
        f = self.filehandles[fh]
        path = self._fixpath(path)
        try:
            treedata=getTree(path)
        except:
            raise FuseOSError(errno.ENOENT)
        if isinstance(treedata[0],dict):
            raise FuseOSError(error,ENOENT)
        treedata[0]=f.read()
        f.flush()

    @log
    def release(self, path, fh):
        f = self.filehandles[fh]
        f.close()
        del self.filehandles[fh]

    @log
    def fsync(self, path, fdatasync, fh):
        '''Flushing seems not to work for some reason.'''
        return self.flush(path,fh)


def mqttworker(tree):
    MQTTClient(tree,'arbiter')

def fuseworker(tree,mountpoint):
    FUSE(MQTTFS(tree,'arbiter'), mountpoint, threads=False, foreground=True)

def main(tree, mountpoint):
    jobs=[]
    jobs.append(Process(target=mqttworker,args=(tree,)))
    jobs.append(Process(target=fuseworker,args=(tree,mountpoint)))
    for job in jobs:
        job.start()
    for job in jobs:
        job.join()


if __name__ == '__main__':
    manager = Manager()
    tree = manager.list([{},time.time(),time.time()])
    main(tree, sys.argv[1])


