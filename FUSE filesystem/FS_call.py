import time
import sys
import bz2
import random
from db.SQLiteHandler import SQLiteHandler


class FileSystem:

	#------------------Constructor makes connection to the database----------------------

	def __init__(self):
		self.sql = SQLiteHandler('seFS')
		self.sql.connect()
		

	#-----------------This functions performs the same as ls operation-------------------------

	def ls(self):
		rows = self.sql.execute(''' select * from metadata ''')
		list = []
		for row in rows:
			list.append(row[1])
		return list


	#-------------This is a getter method for retrieving utime from the database----------------

	def getutime(self, path):
		#Getter for utime
		rows = self.sql.execute('''select mtime, ctime, atime from metadata where abspath='%s' ''' %( path))
		for row in rows:
			tup = (row[0], row[1], row[2])
			return tup


	#----------------This is a setter method for storing utime in the database-------------------

	def utime(self, path, times):
		#Setter for utime
		mtime, ctime, atime = times
		self.sql.execute(''' update metadata  set mtime='%s', ctime='%s', atime='%s' where abspath='%s' ''' %(mtime, ctime, atime, path))
		self.sql.commit()
		return times


	#----------------This is a setter method for storing inode in the database-------------------

	def setinode(self,path,inode):
		#Setter for inode
		self.sql.execute(''' update metadata  set inode = '%d' where abspath='%s' ''' %(inode, path))
		self.sql.commit()
		return inode


	#----------This is a setter method for storing uid, gid, mode and linkcount-------------------

	def set_id(self,path,gid,uid,mode,link):
		#Setter for uid, gid, mode and linkcount
		self.sql.execute(''' update metadata  set gid= '%d',uid= '%d',mode='%d',linkcount='%d' where abspath='%s' ''' %(gid, uid, mode,link, path))
		self.sql.commit()
		return True


	#-----------------------This is a setter method for storing linkcount--------------------------

	def setlinkcount(self,path,linkcnt):
		#Setter for linkcount
		self.sql.execute(''' update metadata  set linkcount = '%d' where abspath='%s' ''' %(linkcnt, path))
		self.sql.commit()
		return linkcnt


	#-------------------------This is a setter method for storing mode-----------------------------

	def setmode(self,path,mode):
		#Setter for mode
		self.sql.execute(''' update metadata  set mode = '%d' where abspath='%s' ''' %(mode, path))
		self.sql.commit()
		return mode


	#------------This is a getter method for retrieving the inode from the database-----------------

	def getinode(self,path):
		"""
			Getter for inode
		"""
		rows = self.sql.execute(''' select inode from metadata where abspath='%s' ''' %( path))
		for row in rows:
			return row[0]


	#----------This is a getter method for retrieving the length of data from the database---------------

	def getlength(self,path):
		"""
			Calculates length of data
		"""
		rows = self.sql.execute(''' select length from metadata where abspath='%s' ''' %( path))
		for row in rows:
			return row[0]


	#------------This is a getter method for retrieving the linkcount from the database-----------------

	def getlinkcount(self,path):
		""" Getter for linkcount """
		rows = self.sql.execute(''' select linkcount from metadata where abspath='%s' ''' %( path))
		for row in rows:
			return row[0]


	#------------This is a getter method for retrieving mode value from the database-------------------

	def getmode(self,path):
		""" Getter for mode """
		rows = self.sql.execute(''' select mode from metadata where abspath='%s' ''' %( path))
		for row in rows:
			return row[0]


	#---------------------------This method inserts new record in database-----------------------------

	def open(self, path):
		""" insert new record in database """

		self.sql.execute(''' insert into metadata (abspath) select '%s' where not exists (SELECT 1 FROM metadata WHERE abspath='%s') ''' %(path,path))
		self.sql.commit()
		return path
	

	#-------------------------------This method implements write() call-------------------------------

	def write(self, path, data):
		""" Implements write() call """
		length  = len(data)

		rows = self.sql.execute(''' select inode from metadata where abspath='%s' ''' %(path) )
		id = None
		for row in rows:
			id = row[0]

			self.sql.execute(''' update metadata set length= '%s',data= '%s' where abspath='%s' ''' %(length, data, path))
			self.sql.commit()
			return


	#----------------------This method sets mtime, atime for the file in the database-----------------

	def set_writetime(self, path, times):

		self.sql.execute(''' update metadata  set mtime='%s', atime='%s' where abspath='%s' ''' %(times, times, path))
		self.sql.commit()
		return times
	

	#--------------------------This method deletes the file from the directory-------------------------
	
	def remove(self,abspath):
		"""
			Implements unlink() call
		"""
		self.sql.execute(''' delete from metadata where abspath='%s' ''' %(abspath))
		self.sql.commit()
		return
	

	#-------------------Look for inode in the database for further processing---------------------------

	def search(self, abspath):
		""" Search file in database record """
		rows = self.sql.execute(''' select inode from metadata where abspath='%s' ''' %(abspath))
		if rows.fetchall():
			return True
		return False


	#--------------------Retrieve the data from its corresponding file based on its inode---------------

	def read(self, fh):
		"""
			Implements read() call
		"""
		id  = None
		rows = self.sql.execute(''' select inode from metadata where abspath='%s' ''' %(fh))
		for row in rows:
			id = row[0]

		if id is None:
			return None
		rows = self.sql.execute(''' select data from metadata where abspath='%s' ''' %(fh))
		for row in rows:
			return row[0]
		
		
	#-------------------------Now this method closes the database connection------------------------------

	def __del__(self):
		"""
			Closes the databse connection
		"""
		self.sql.close()



