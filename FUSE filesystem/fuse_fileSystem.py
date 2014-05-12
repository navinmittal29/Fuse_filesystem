import errno
import fuse
import stat
import time
import os
from FS_call import FileSystem

try:
    from cStringIO import StringIO
except:
    from StringIO import StringIO

fuse.fuse_python_api = (0, 2)

class Buffer:


  def __init__(self):
    self.buf = cStringIO.StringIO()
    self.dirty = False

  def __getattr__(self, attr, default=None):
    return getattr(self.buf, attr, default)

  def __len__(self):
    position = self.buf.tell()
    self.buf.seek(0, os.SEEK_END)
    length = self.buf.tell()
    self.buf.seek(position, os.SEEK_SET)
    return length

  def truncate(self, *args):
    if len(self) > self.buf.tell():
      self.dirty = True
    return self.buf.truncate(*args)

  def write(self, *args):
 
    self.dirty = True
    return self.buf.write(*args)


class MyFS(fuse.Fuse):
    def __init__(self, *args, **kw):
        fuse.Fuse.__init__(self, *args, **kw)

      
        self.flags = 0
        self.multithreaded = 0
	self.fd = 0
	self.gid = os.getgid()
	self.uid = os.geteuid()

	self.FSobj = FileSystem()
	self.FSobj.open('/')
	return_value = self.FSobj.search('/')
	if return_value is False:
		print "Created root with %s" %return_value
		self.FSobj.write('/', "Root")
		times = int(time.time())
		mytime = (times, times, times)
		return_value = self.FSobj.utime('/', mytime)

		self.FSobj.setinode('/', 1)
		self.FSobj.setmode('/', stat.S_IFDIR | 0755)
		self.FSobj.setlinkcount('/', 2)
		self.is_dirty = False

  	'''  def getdir(self, path):
	print "in getdir"
  	'''
    def getattr(self, path):
	FSobj = FileSystem()
        st = fuse.Stat()
	c  = fuse.FuseGetContext()

       	return_value = FSobj.search(path)
	print "Already present =", return_value 
	if return_value is True:
		st.st_ino = int(FSobj.getinode(path))
		st.st_uid, st.st_gid = (c['uid'], c['gid'])
		st.st_mode = FSobj.getmode(path)
		st.st_nlink = FSobj.getlinkcount(path)

		if FSobj.getlength(path) is not None:
			st.st_size = int(FSobj.getlength(path))
		else:
			st.st_size = 0

		tup = FSobj.getutime(path)
		st.st_mtime = int(tup[0].strip().split('.')[0])
		st.st_ctime = int(tup[1].strip().split('.')[0])
		st.st_atime = int(tup[2].strip().split('.')[0])

		
		print "inode numder = %d" %st.st_ino
		

		return st
	else:
       		return - errno.ENOENT
    

	#----------------executable----------------------------------

    def readdir(self, path, offset):			
	print "readdir", path
	yield fuse.Direntry('.')
        yield fuse.Direntry('..')

	FSobj = FileSystem()
	all_files = FSobj.ls()
	print "Rest of the files in root dir"

	for i in all_files:
		
		if str(i) == path:
			continue

		if (len(i.split(path))==2):
			  print "%s" %i
			  strpath = i.split(path)
			  strpath = strpath[1]

			  if path == '/':     		
			  	yield fuse.Direntry(str(i[1:]))
			  elif (len(strpath.split('/')) > 2):				
				continue
			  else:
				size=len(path) + 1
				yield fuse.Direntry(str(i[size:]))

    
    def open(self, path, flags):
	FSobj = FileSystem()
	return_value = FSobj.search(path)
	if return_value is True:
		return 0
	return -errno.ENOENT


	#----------------executable----------------------------------
    
    def mkdir(self, path, mode):			
	flags = 1
	self.create(path, mode| stat.S_IFDIR,flags)
	return 0


	#----------------executable----------------------------------

    def rmdir(self, path):				
	print "In rmDir"
	FSobj = FileSystem()
	return_value = FSobj.remove(path)
	return 0

    def create(self, path, mode, flags):
	FSobj = FileSystem()
	return_value = self.open(path, flags)

	if return_value == -errno.ENOENT:
		return_value = FSobj.open(path)
		print "Creating the file %s" %path
		current_time = int(time.time())
		new_time = (current_time, current_time, current_time)
		return_value = FSobj.utime(path, new_time)

		self.fd = len(FSobj.ls())
		print "In create:fd = %d" %(self.fd)
		FSobj.setinode(path, self.fd)

		st = fuse.Stat()
		if path == '/':
			st.st_nlink = 2
	       		st.st_mode = stat.S_IFDIR | 0755
		
		if flags == 1:
			st.st_nlink = 2
	       		st.st_mode = stat.S_IFDIR | 0755
		else:
			st.st_mode = stat.S_IFREG | 0777
			st.st_nlink = 1

		FSobj.set_id(path, self.gid, self.uid,st.st_mode,st.st_nlink)


	else:
		print "The file %s exists!!" %path
	return 0

    def write(self, path, data, offset):
	print "In write path=%s" %path
	length = len(data)
	print "The data is %s len=%d offset=%d" %(str(data), length, offset)

	FSobj = FileSystem()
	prev = FSobj.read(path)
	data = prev + data
	return_value = FSobj.write(path, data)
	
	current_time = int(time.time())
	return_value = FSobj.set_writetime(path, current_time)

	return length

    def release(self, path, flags):
	print "In release"
	if self.is_dirty is True:
		print "Flushing buffer"
		print self.buf.read()
		FSobj = FileSystem()
		return_value = FSobj.write(path, self.buf.read())
		print self.buf.read()
		#self.buf.close()
		#del self.buf
		self.is_dirty = False
		print return_value
	return 0

    def access(self, path, flag):
        print "access path=%s" %path
	FSobj = FileSystem()
	if FSobj.search(path) is True:
		print "In access, found the file %s" %path
		return 0
	else:
		print "Could not find the file %s" %path
		return -errno.EACCES

    def read(self, path, size, offset):
	try:
		print "In read %s %d %d" %(path, size, offset)
		FSobj = FileSystem()
		return_value = FSobj.read(path)
		print "read(): %s" %(return_value[:-1])
		fbuf = StringIO()
		fbuf.write(str(return_value[:-1]))
		return fbuf.getvalue()
	except Exception, e:
		print "read failed"
		return e


	#----------------executable----------------------------------

    def chmod(self, path, mode):				
	print "In chmod %s %s" %(path, str(mode))
	self.FSobj.setmode(path, stat.S_IFREG | mode)

        return 0

	#----------------executable----------------------------------

    def chown(self, path, uid, gid):				
	print "In chown %s %d %d" %(path, uid, gid)
        #self.files[path]['st_uid'] = uid
	#self.files[path]['st_gid'] = gid
	return 0


    def utime(self, path, times=None):
	atime, mtime = times
	FSobj = FileSystem()
	return_value = FSobj.utime(path, (atime, mtime, atime))
	return 0


	#----------------executable----------------------------------

    def unlink(self,path):					
	print "In unlink path %s" %path
	FSobj = FileSystem()
	return_value = FSobj.remove(path)
	return

    def fgetattr(path, fh=None):
	print "In fgetattr"
	return 0

if __name__ == '__main__':
    fs = MyFS()
    fs.parse(errex=1)
    fs.main()
