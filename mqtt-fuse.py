#!/usr/bin/env python
import os, sys, errno, time
import pwd, grp, resource
import StringIO

from fuse import FUSE, FuseOSError, Operations
from multiprocessing import Process
import paho.mqtt.client as mqtt

def log(f):
    '''Decorator function for debugging.'''
    def my_f(*args, **kwargs):
        print (f.__name__)
        print (args)
        return f(*args, **kwargs)
    return my_f


class TreeData():
    def __init__(self,payload=None,mtime=None):
        self.payload=payload
        self.ctime=time.time()
        if mtime:
            self.mtime=mtime
        else:
            self.mtime=time.time()

    def __str__(self):
        if isinstance(self.payload,dict):
           if len(self.payload)<1: return '{}'
           return '{'+','.join([x+':'+str(self.payload[x]) for x in self.payload])+'}'
        else:
           return str(self.payload)

tree=TreeData({})

def putTree(path,payload):
    global tree
    currpath=tree
    for n,p in enumerate(path):
        if p not in currpath.payload and isinstance(currpath.payload,dict):
            currpath.payload[p]=TreeData({})
        else:
            currpath.mtime=time.time()
        if n == len(path)-1:
            t=TreeData()
            t.payload=payload
            t.mtime=time.time()
            currpath.payload[p]=t
        else:
            currpath = currpath.payload[p]
#    print(tree)

def getTree(path):
    global tree
    currpath=tree
    for p in path:
        currpath=currpath.payload[p]
    return currpath

class MQTTClient():
    def __init__(self,host,port=1883):
        self.mqtt = mqtt.Client()
        self.mqtt.on_connect=self.on_connect
        self.mqtt.on_message=self.on_message
        self.mqtt.connect(host,port)
        self.mqtt.loop_forever() #BLOCKING

    def on_connect(self, client, userdata, flags, rc):
        client.subscribe("#")

    def on_message(self, client, userdata, msg):
        parts = filter(bool,msg.topic.split('/'))
        putTree(parts,msg.payload)

class MQTTFS(Operations):
    def __init__(self):
        self.filehandles={}
        self.fhmax=0

    def _fixpath(self, path):
        return filter(bool,os.path.split(path))

    @log
    def access(self, path, mode):
        return true

    @log
    def chmod(self, path, mode):
        raise FuseOSError(errno.EPERM)

    @log
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
        data['st_atime']=int(treedata.mtime)
        data['st_mtime']=int(treedata.mtime)
        data['st_ctime']=int(treedata.ctime)
        data['st_gid']=os.getgid()
        mode = int(777,8)
        if isinstance(treedata.payload,dict):
            data['st_mode']=mode + 16384
            data['st_size']=st.st_size
        else:
            data['st_mode']=mode + 32768
            data['st_size']=len(treedata.payload())
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
        if isinstance(treedata.payload,dict):
            return ['.','..']+treedata.payload.keys()

    def readlink(self, path):
        '''No symlinks in mqtt.'''
        return path

    def mknod(self, path, mode, dev):
        '''No special devices in mqtt.'''
        raise FuseOSError(errno.EACCES)

    @log
    def rmdir(self, path):
        '''remove dir if empty.'''
        path = self._fixpath(path)
        try:
            treedata=getTree(path)
        except:
            raise FuseOSError(errno.ENOENT)
        if isinstance(treedata.payload,dict):
            if len(treedata.payload)==0:
                getTree(path[:-1]).payload.remove(path[-1])
            else:
                raise FuseOSError(errno.ENOTEMPTY)

    @log
    def mkdir(self, path, mode):
        '''Make a new directory.'''
        path = self._fixpath(path)
        try:
            treedata=getTree(path[-1])
        except:
            raise FuseOSError(errno.ENOENT)
        if isinstance(treedata.payload,dict):
            treedata.payload[path[-1]]=TreeData({})

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
            treedata.mtime=time.time()
        else:
            treedata.mtime=times[1] #(atime,mtime)

    @log
    def open(self, path, flags):
        '''Open a filehandle as an IObuffer.'''
        return self.create(path,flags)

    @log
    def create(self, path, mode, fi=None):
        '''Open a nonexistent file. This will just create a new file and generate a
        new filehandle for you.'''
        path = self._fixpath(path)
        try:
            treedata=getTree(path[-1])
        except:
            raise FuseOSError(errno.ENOENT)
        if not isinstance(treedata.payload,dict):
            raise FuseOSError(error,ENOENT)
        treedata.payload[path[-1]]=TreeData('')
        if len(self.filehandles) == maxfh:
            raise FuseOSError(errno.EMFILE)
        while self.fhmax in self.filehandles.keys():
            self.fhmax = (self.fhmax+1)%maxfh
        self.filehandles[self.fhmax] = StringIO.StringIO()
        return self.fhmax

    @log
    def read(self, path, length, offset, fh):
        f = self.filehandles[fh]
        f.seek(offset)
        return f.read(length)

    @log
    def write(self, path, buf, offset, fh):
        '''Write to the object, and publish here.'''
        f = self.filehandles[fh]
        return f.write(f.read(offset)+buf)

    @log
    def truncate(self, path, length, fh=None):
        if fh is None:
            fh = self.fhmax
        f.trunc(length)

    @log
    def flush(self, path, fh):
        '''Empty the buffer.'''
        f = self.filehandles[fh]
        path = self._fixpath(path)
        try:
            treedata=getTree(path)
        except:
            raise FuseOSError(errno.ENOENT)
        if isinstance(treedata.payload,dict):
            raise FuseOSError(error,ENOENT)
        treedata.payload=f.read()
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

def mqttworker():
    MQTTClient('arbiter')

def fuseworker(mountpoint):
    FUSE(MQTTFS(), mountpoint, nothreads=True, foreground=True)

def main(mountpoint):
    jobs=[]
    jobs.append(Process(target=mqttworker))
    jobs.append(Process(target=fuseworker,args=(mountpoint,)))
    for job in jobs:
        job.start()


if __name__ == '__main__':
    main(sys.argv[1])


