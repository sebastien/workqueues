#!/usr/bin/env python
# -----------------------------------------------------------------------------
# Project   : Workqueues
# -----------------------------------------------------------------------------
# Author    : Sebastien Pierre                            <sebastien@ffctn.com>
# License   : Revised BSD License
# -----------------------------------------------------------------------------
# Creation  : 21-Jun-2012
# Last mod  : 21-Jun-2012
# -----------------------------------------------------------------------------

import os, threading, subprocess

# Hooks for custom logging functions
log  = lambda *_:_
warn = lambda *_:_
err  = lambda *_:_

# -----------------------------------------------------------------------------
#
# FREQUENCY
#
# -----------------------------------------------------------------------------

class Frequency:

	@staticmethod
	def Days(count=1):
		return Frequency._Format("D", count)

	@staticmethod
	def Weeks(count=1):
		return Frequency._Format("W", count)

	@staticmethod
	def Months(count=1):
		return Frequency._Format("M", count)

	@staticmethod
	def Years(count=1):
		return Frequency._Format("Y", count)

	@staticmethod
	def _Format(letter,count=1):
		if count == 1: return letter
		else: return letter + str(count)

# -----------------------------------------------------------------------------
#
# RESULTS
#
# -----------------------------------------------------------------------------

class Result:

	def __init__( self ):
		self.duration = -1
		self.started  = None

	def isSuccess( self ):
		return False

	def isFailure( self ):
		return False

	def isTimeout( self ):
		return False

class Success:

	def __init__( self ):
		Result.__init__(self)

	def isSuccess( self ):
		return True

class Failure:

	def __init__( self ):
		Result.__init__(self)

	def isFailure( self ):
		return True

class Timeout(Failure):

	def __init__( self ):
		Failure.__init__(self)

	def isTimeout( self ):
		return True

# -----------------------------------------------------------------------------
#
# JOB
#
# -----------------------------------------------------------------------------

class Job:
	"""
	- `timeout`   indicates how long (in s) the job can run without being stopped
	- `scheduled` is a (Y,M,d,h,m) UTC tuple telling when the job should be started
	- `until`     is a (Y,M,d,h,m) UTC tuple after which the job should not be started
	- `frequency` either a number (frequency in seconds) or one of (w)eek (d)ay (m)onth.
	- `repeat`    the number of times the job should be repeated.
	"""

	SUBMITTED    = "submitted"
	RESUBMITTED  = "resubmitted"
	SELECTED     = "selected"
	IN_PROCESS   = "inprocess"

	DATA = []
	def __init__( self ):
		self.timeout   = -1
		self.scheduled = -1
		self.submitted = -1
		self.until     = -1
		self.frequency = None
		self.repeat    = -1
		self.id        = None
		assert "id" not in self.DATA, "DATA does not allow an 'id' attribute"

	def setID( self, jobID ):
		assert self.id is None, "Job already has a job ID."
		self.id = jobID
		return self

	def isNow( self ):
		pass

	def getNextDate( self ):
		pass

	def export( self ):
		data = {}
		base = dict(
			timeout   = self.timeout,
			scheduled = self.scheduled,
			until     = self.until,
			frequency = self.frequency,
			repeat    = self.repeat,
			data      = data
		)
		for field in self.DATA: data[field] = getattr(self, field)
		return base
	
# -----------------------------------------------------------------------------
#
# WORKER
#
# -----------------------------------------------------------------------------

class Worker:

	AS_FUNCTION = "function"
	AS_THREAD   = "thread"
	AS_PROCESS  = "process"

	def __init__( self ):
		self.job = None

	def setJob( self, job ):
		"""Workers can only be assigned one job at a time."""
		assert self.job is None
		self.job = job

	def run( self ):
		assert not self.isAvailable(), "No job set for this worker"
		# NOTE: Here the run method CAN NOT FAIL. It ALWAYS have to
		# return a result, which is either a Success, Failure or 
		# Timeout.

	def _runAsFunction( self, job ):
		pass

	def _runAsThread( self, job ):
		pass

	def _runAsProcess( self, job ):
		pass

	def isAvailable( self ):
		return self.job is None

# -----------------------------------------------------------------------------
#
# WORKER POOL
#
# -----------------------------------------------------------------------------

class Pool:
	"""Pools are used to limit the number of elements (workers executing)
	at once."""

	def __init__(self, capacity):
		self.capacity = capacity
		self.elements = []

	def submit(self, job, sync=False):
		# FIXME: This should be a blocking operation, and should only return
		# a worker when it's available
		if self.canAdd():
			worker = Worker().setJob(job)
			self.elements.append(worker)
			return worker
		else:
			return None

	def canAdd(self):
		return len(self.elements) < self.capacity

	def remove(self, element):
		assert element in self.elements
		self.elements.remove(element)

	def count(self):
		return len(self.elements)

	def log( self, *args ):
		pass

	def warn( self, *args ):
		pass

	def err( self, *args ):
		pass
	
# -----------------------------------------------------------------------------
#
# SCHEDULER
#
# -----------------------------------------------------------------------------

class Scheduler:

	def __init__( self ):
		pass
			
	def select( self, queue ):
		pass

	def run( self ):
		pass

# -----------------------------------------------------------------------------
#
# INCIDENT
#
# -----------------------------------------------------------------------------

class Incident:

	def __init__( self ):
		pass

	def isAboveThreshold( self ):
		return False

# -----------------------------------------------------------------------------
#
# QUEUE
#
# -----------------------------------------------------------------------------

# FIXME: Separate MemoryQueue of the abstract queue
class Queue:

	# FIXME: Add support for that
	MAX_JOBS   = 128000

	def __init__( self ):
		self.pool  = None
		self.jobs  = {}
		# FIXME: Should be aware of the queue's max number of jobs
		self.index = 0
		self._lastProcessed = -1

	def setPool( self, pool ):
		self.pool = pool

	def submit( self, job ):
		"""Submit a new job or a list of jobs in this queue"""
		if type(job) in (tuple, list):
			return map(self.submit, job)
		return self._addJob(job)

	def resubmit( self, job ):
		assert job.id != None
		self._updateJobStatus(job, Job.RESUBMITTED)

	def remove( self, job ):
		assert job.id != None
		self._updateJobStatus(job, Job.REMOVED)

	def list( self, until=None, since=None, status=None ):
		"""Lists the jobs in the job queue, by ascending chronological order"""
		pass

	def failed( self, job, failure ):
		incident = self._getIncident(job, failure)
		if incident.isAboveThreshold():
			self.remove(job)
		else:
			self.resubmit(job)
		return incident

	def iterate( self, count=-1 ):
		print "HAS JOBS", self._hasJobs()
		while (count == -1 or count > 0) and self._hasJobs():
			# Takes the next available job
			job = self._getNextJob()
			print "JOB", job
			# Makes sure it's time to execute it
			# ...if not, we return the time we have to wait up until the next event
			# Makes sure the pool can process the event
			# ...if not, we return the maximum time in which the pool will be free/or a callback to when the pool will be free
			worker = self.pool.submit(job)
			# Now we have the worker and we ask it to run the job
			result = worker.run()
			if   isinstance(result, Success):
				# the job is successfully processed
				self._jobProccessed(job)
			elif isinstance(result, Failure):
				self.failed(job, result)
			else:
				self.failed(job, UnexpectedResult(result))
			yield result
			if count > 0: count -= 1

	def run( self, count=-1 ):
		for _ in self.iterate(count):
			print _

	def _getNextJob( self ):
		"""Returns the next job and sets it as selected in this queue"""
		job = self._getJob(self._lastProcessed + 1)
		self._updateJobStatus(job, Job.SELECTED)
		return job

	def _hasJobs( self ):
		"""Tells if there are still jobs submitted in the queue"""
		return self.jobs

	def _addJob( self, job ):
		"""Adds a new job to the queue"""
		# FIXME: Should be synchronized
		i = self.index
		assert not self.jobs.has_key(i)
		self.jobs[i] = job
		job.setID(i)
		self.index += 1
		return i

	def _jobProccessed( self, jobOrJobID ):
		i = self._id(jobOrJobID)
		self._lastProcessed = i
		del self.jobs[i]
		return i

	def _updateJobStatus( self, jobOrJobID, status ):
		"""Updates the status of this job."""

	def _getJob( self, jobOrJobID ):
		"""Gets the job with the given id from the job queue."""
		print self.jobs.keys()
		i = self._id(jobOrJobID)
		return self.jobs[i]

	def _id( self, jobOrJobID ):
		if isinstance(jobOrJobID, Job):
			res = jobOrJobID.id
			assert res is not None, "Job has no id"
			return res
		else:
			return jobOrJobID 

# -----------------------------------------------------------------------------
#
# DIRECTORY QUEUE
#
# -----------------------------------------------------------------------------

class DirectoryQueue(Queue):

	def __init__( self, path ):
		Queue.__init__(self)
		self.path = path
		# We create the directory if it does not exist
		if not os.path.exists(path):
			os.makedirs(path)

# -----------------------------------------------------------------------------
#
# BEANSTALK QUEUE
#
# -----------------------------------------------------------------------------

class BeanstalkQueue(Queue):
	pass

# -----------------------------------------------------------------------------
#
# ZEROMQ QUEUE
#
# -----------------------------------------------------------------------------

class ZMQueue(Queue):
	pass

# -----------------------------------------------------------------------------
#
# HTTP QUEUE
#
# -----------------------------------------------------------------------------

class HTTPQueue(Queue):
	pass

# EOF - vim: tw=80 ts=4 sw=4 noet
