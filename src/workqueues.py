#!/usr/bin/env python
# -----------------------------------------------------------------------------
# Project   : Workqueues
# -----------------------------------------------------------------------------
# Author    : Sebastien Pierre                            <sebastien@ffctn.com>
# License   : Revised BSD License
# -----------------------------------------------------------------------------
# Creation  : 21-Jun-2012
# Last mod  : 19-Feb-2016
# -----------------------------------------------------------------------------

import os, threading, subprocess, time, datetime, sys, json, traceback, signal
import cStringIO as StringIO
try:
	from   retro.core import asJSON
except ImportError:
	import json.dumps as     asJSON
try:
	import reporter
except ImportError:
	import logging as reporter

# FIXME: Failed jobs seem to stay forever in the queue without being removed
# FIXME: Deal with Corrupt job files!

# TODO: Add Runner 1) Inline 2) Thread 3) Process 4) TMUX
# TODO: Generator support in Job's run() to divide work?
# TODO : Add support for storing the job's results -- queues should probably
# have different queues: backburner (for not schedules), incoming:(to be processed),
# processing(being processed), failed:(jobs failed and permanently removed), completed:(completed)
# FIXME: Make sure that exceptions are well caught everywhere. For instance
# DirectoryQueue will just fail if it cannot decode the JSON

__version_ = "0.7.3"
__doc__    = """\
"""

# Hooks for custom logging functions
logging = reporter.bind("workqueues")

# Execution modes for jobs/workers
AS_FUNCTION      = "function"
AS_THREAD        = "thread"
AS_PROCESS       = "process"
SHELL_ESCAPE     = " '\";`|><&\n"

# States for jobs
# JOB:
#
# 	STATES = dict(
# 		"submitted"   : ("resubmitted", "completed", "failed")
# 		"resubmitted" : ("resubmitted", "completed", "failed")
# 		"failed"      : None
# 		"completed""  : None
# 	)

JOB_SUBMITTED    = "submitted"
JOB_RESUBMITTED  = "resubmitted"
# FIXME: Maybe IN_PROCESS shoud be running?
JOB_IN_PROCESS   = "inprocess"
JOB_COMPLETED    = "completed"
JOB_FAILED       = "failed"
JOB_REMOVED      = "removed"
JOB_CLASSES      = {}

def timestamp():
	"""Returns the current time as an UTC timestamp in seconds"""
	return time.time() - time.timezone

def shellsafe( path ):
	"""Makes sure that the given path/string is escaped and safe for shell"""
	return "".join([("\\" + _) if _ in SHELL_ESCAPE else _ for _ in path])

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

	@classmethod
	def Import( self, data ):
		result_class    = eval(data["type"])
		result          = result_class(data["value"])
		result.duration = data["duration"]
		result.started  = data["started"]
		return result

	def __init__( self, value, job=None ):
		self.duration = -1
		self.started  = timestamp()
		self.value    = value
		self.job      = job.id if isinstance(job, Job) else job

	def isSuccess( self ):
		return False

	def isFailure( self ):
		return False

	def isTimeout( self ):
		return False

	def isPending( self ):
		return False

	def happenedAfter( self, t ):
		assert type(t) is float, "The given time should be given in seconds since UTC"
		return self.started >= t

	def happenedBefore( self, t ):
		assert type(t) is float, "The given time should be given in seconds since UTC"
		return self.started < t

	def export( self ):
		return dict(
			type=self.__class__.__name__.split(".")[-1],
			value=self.value,
			started=self.started,
			duration=self.duration,
		)

class Success(Result):

	def __init__( self, value, job=None ):
		Result.__init__(self, value, job)

	def isSuccess( self ):
		return True

class Failure(Result):

	def __init__( self, description=None, value=None, job=None):
		Result.__init__(self, value, job)
		self.description = description

	def isFailure( self ):
		return True

	def __str__(self):
		res = "%s:<%s>" % (self.__class__.__name__, self.description)
		if self.value: res += " (%s)"  % (self.value)
		if self.job:   res += " in %s" % (self.job)
		return res

class UnexpectedResult(Failure):

	def __init__( self, result, job=None ):
		Failure.__init__(self, value=result, job=job)

class Timeout(Failure):

	def __init__( self, value ):
		Failure.__init__(self, value)

	def isTimeout( self ):
		return True

class Future(Result):

	def __init__( self ):
		Result.__init__(self, None)
		self.callback = None

	def isPending( self ):
		return True

	def set( self, value ):
		self.value = value
		if self.callback: self.callback(value)
		return self

	def onset( self, callback):
		self.callback = callback
		return self

	def isSuccess( self ):
		return self.value and self.value.isSuccess()

	def isFailure( self ):
		return self.value and self.value.isFailure()

	def isTimeout( self ):
		return self.value and self.value.isTimeout()


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
	- `retries`   the number of times the job was tried to run in the current cycle
	"""

	DATA          = []
	RUN           = AS_FUNCTION
	RETRIES       = 5
	RETRIES_DELAY = (60 * 1, 60 * 5, 60 * 10, 60 * 15)
	CORE_PROPERTIES = (
		"timeout",
		"scheduled",
		"submitted",
		"until",
		"frequency",
		"repeat",
		"id",
		"retries",
		"lastRun",
		"type",
		"status",
		"out",
		"err",
	)
	PROPERTIES = ()

	@classmethod
	def Registered( cls, jobClass ):
		cls.Register(jobClass)
		return jobClass

	@classmethod
	def Register( cls, *jobClasses ):
		for job_class in jobClasses:
			name = job_class.__module__ + "." + job_class.__name__.split(".")[-1]
			if not JOB_CLASSES.has_key(name):
				JOB_CLASSES[name] = job_class
		return jobClasses[0] if len(jobClasses) == 1 else jobClasses

	@classmethod
	def GetClass( cls, name ):
		# NOTE: Here, we could simply use `eval` os even sys.modules to resolve
		# the class, but by doing this we rely on explicit job registration.
		# The main reason is that jobs might be insecure, so we want to limit
		# the possibility to execute arbitrary code.
		if not name:
			return None
		elif JOB_CLASSES.has_key(name):
			return JOB_CLASSES[name]
		else:
			try:
				job_class = eval(name)
				return job_class
			except NameError:
				return None

	@classmethod
	def Import( cls, export, jobID=None ):
		"""Imports a job that was previously exported."""
		job_class = cls.GetClass(export["type"])
		if job_class:
			job       = job_class()
		else:
			job       = Job()
			job.isUnresolved = export
		# NOTE: The following list is the list of all the Job's properties
		for k in cls.CORE_PROPERTIES:
			if export.has_key(k):
				setattr(job, k, export[k])
		for k in cls.PROPERTIES:
			if export.has_key(k):
				setattr(job, k, export[k])
		if jobID: job.id  = jobID
		if export.get("result"):
			result_json = export["result"]
			result = Result.Import(result_json)
			job.result = result
		for _,v in export["data"].items():
			setattr(job, _, v)
		return job

	@classmethod
	def Shell( cls, command, cwd=".", env=None ):
		if type(command) in (tuple, list): command = " ".join(command)
		# FIXME: Subprocess is sadly a piece of crap, cmd.wait() can still
		# be blocking in cases where os.popen() would not block at all.
		# NOTE: You might want to specify env env=dict(LANG="C")
		if env:
			cmd      = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=cwd, env=env)
		else:
			cmd      = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=cwd)
		# NOTE: is the status really necessary?
		status   = cmd.wait()
		res, err = cmd.communicate()
		if status == 0:
			return (res, err)
		else:
			return (res, err)

	def __init__( self, env=None, **kwargs ):
		self.timeout      = -1
		self.scheduled    = -1
		self.submitted    = -1
		self.until        = -1
		self.frequency    = None
		self.repeat       = -1
		self.id           = None
		self.retries      = 0
		self.lastRun      = -1
		self.progress     = -1
		self.env          = env
		self.isUnresolved = False
		self.type         = self.__class__.__module__ + "." + self.__class__.__name__.split(".")[-1]
		self.status       = None
		self.result       = None
		self.err          = ""
		self.out          = ""
		self._onProgress  = []
		for k,v in kwargs.items():
			if k in self.CORE_PROPERTIES or self.PROPERTIES:
				setattr(self, k, v)
			else:
				err("Job {0} does not define a property {1}".format(self, k))
		assert "id" not in self.DATA, "DATA does not allow an 'id' attribute"

	def setStatus( self, status ):
		old_status  = self.status
		self.status = status
		return old_status

	def onProgress( self, callback ):
		self._onProgress.append(callback)

	def updateProgress( self, progress ):
		self.progress = progress
		try:
			for _ in self._onProgress:
				_(progress,_)
		except Exception, e:
			err("Exception in job progress callback: " + str(e))

	def isInProcess( self ):
		return self.status == JOB_IN_PROCESS

	def isComplete( self ):
		return self.result and isinstance(self.result, Success)

	def hasFailed( self ):
		return self.result and isinstance(self.result, Failure)

	def getRunType( self ):
		"""Returns the run type of this job, as defined by the job's `RUN` attribute"""
		return self.RUN

	def setID( self, jobID ):
		"""Sets the ID that is used to represent this job in the queue it's added to.
		The consequence of this is that a job cannot be added to multiple queues at
		the same time."""
		assert self.id is None, "Job already has a job ID."
		self.id = jobID
		return self

	def setResult( self, result ):
		"""Sets the result obtained from running this job, wrapping it in
		a Result instance if necessary."""
		if not isinstance(result, Result): result = Result(result)
		self.result = result
		return result

	def canRetry( self ):
		"""Tells if this job can be retried (ie. `self.retries < self.RETRIES`)"""
		return self.retries < self.RETRIES

	def getRetryDelay( self ):
		"""Returns the delay in seconds until the next retry"""
		i = max(0, min(len(self.RETRIES_DELAY), self.retries) - 1)
		return self.RETRIES_DELAY[i]

	def run( self ):
		"""The main Job function that you'll override when implementing the Job"""
		raise Exception("Job.run not implemented")

	def shell( self, command, cwd=".", env=None):
		out, err = self.Shell(command, cwd, env=env)
		self.out += out
		self.err += err
		return out, err

	def shellsafe( self, value ):
		return shellsafe(value)

	def failure( self, description=None, value=None ):
		return Failure(description=description, value=value, job=self)

	def success( self, value=None ):
		return Success(value or self)

	def export( self ):
		# If the job is unresolved, then we export the same data that was
		# imported
		if self.isUnresolved: return self.isUnresolved
		result_export = None
		# FIXME: We can't include the result in the export because the
		# result includes the Job
		if self.result:
			result_export = self.result.export()
		data = {}
		# FIXME: Not sure if we should exclude PROPERTIES
		for field in self.DATA: data[field] = getattr(self, field)
		base = dict([(k,getattr(self,k)) for k in list(self.PROPERTIES) + list(self.CORE_PROPERTIES)])
		base["data"] = data
		return base

	def __str__( self ):
		data = asJSON(self.export()["data"])
		if len(data) > 80: data = data[:77] + "..."
		if self.isUnresolved:
			return "%s[UNRESOLVED](%s):%s" % (self.isUnresolved["type"], self.id, data)
		else:
			return "%s(%s):%s" % (self.__class__.__name__, self.id, data)

# -----------------------------------------------------------------------------
#
# WORKER
#
# -----------------------------------------------------------------------------

class Worker:

	def __init__( self ):
		self.job       = None
		self._join     = None
		self.onJobEnd  = []
		self.isRunning = False

	def setJob( self, job, onJobEnd=None ):
		"""Workers can only be assigned one job at a time."""
		assert self.job is None, "Worker already has a job assigned"
		self.onJobEnd = []
		if onJobEnd: self.onJobEnd.append(onJobEnd)
		self.job = job
		return self

	@property
	def isAvailable( self ):
		return not self.isRunning

	def run( self, runType=None ):
		assert self.job, "No job set for this worker"
		assert not self.isRunning, "Worker is already running the job"
		# NOTE: Here the run method CAN NOT FAIL or TIMEOUT. It ALWAYS have to
		# return a result, which is either a Success, Failure or Timeout.
		# In practive, if run_type is `AS_FUNCTION` then it can actually
		# fail and timeout, but for other run types, it won't.
		self.isRunning = True
		run_type   = runType or self.job.getRunType()
		assert run_type in (AS_FUNCTION, AS_THREAD, AS_PROCESS), "Unkown run type for worker: %s" % (repr(run_type))
		if   run_type == AS_FUNCTION:
			try:
				result = self._runAsFunction(self.job)
			except Exception, e:
				logging.error("Job {0} failed with exception: {1}".format(self.job, e))
				result = Failure("Job failed with exception %s" % (e))
		elif run_type == AS_THREAD:
			try:
				result = self._runAsThread(self.job)
			except Exception, e:
				logging.error("Job {0} failed with exception: {1}".format(self.job, e))
				result = Failure("Job failed with exception %s" % (e))
		elif run_type == AS_PROCESS:
			result = Failure("Job run type %s not implemented" % (run_type))
		else:
			result = Failure("Uknown job run type: %s" % (run_type))
		# The job is assigned a result
		self.job.setResult(result)
		return result

	def doJobEnd( self, result ):
		logging.trace("Worker: job ended {0}:{1}".format(self.job, result))
		for callback in self.onJobEnd:
			try:
				callback(result, self)
			except Exception, e:
				logging.error("Callback failed on worker's job end: %s for %s in %s" % (callback, result, self))
		self.job       = None
		self.isRunning = False

	def join( self ):
		"""Synchronously waits for the completion of this worker."""
		if self._join:
			self._join()
			self._join = None
		return self

	def _setResultTime( self, result, startTime ):
		end_time = time.time()
		assert isinstance(result, Result)
		result.started  = startTime
		result.duration = end_time - startTime

	# FIXME: These should first detect wether a job returns a success or
	# a failure.
	def _runAsFunction( self, job, future=None ):
		result = None
		start_time = time.time()
		logging.trace("Worker: job started {0}".format(self.job))
		try:
			result = Success(job.run())
			if future: future.set(result)
		except Exception, e:
			error_msg = StringIO.StringIO()
			traceback.print_exc(file=error_msg)
			error_msg = error_msg.getvalue()
			result = Failure(error_msg, job=job)
			if future: future.set(result)
		self._setResultTime(result, start_time)
		if future: self._setResultTime(future, start_time)
		# We have to put the doJobEnd here, as callbacks might fail or
		# take too long
		self.doJobEnd(result)
		return result

	def _runAsThread( self, job ):
		"""Runs the given job as a thread, and returns a `Future` instance."""
		assert not self._join
		result = Future()
		def target(job=job, future=result):
			self._runAsFunction(job, future)
			self._join = None
		thread = threading.Thread(target=target)
		thread.start()
		self._join = lambda: thread.join()
		return result

	def _runAsProcess( self, job ):
		pass


# -----------------------------------------------------------------------------
#
# WORKER POOL
#
# -----------------------------------------------------------------------------

class Pool:
	"""Pools are used to limit the number of elements (workers executing)
	at once."""

	def __init__(self, capacity=5):
		"""Creates a default pool with the given capacity (5 by default)"""
		self.capacity      = capacity
		self._semaphore    = threading.BoundedSemaphore(self.capacity)
		self._activeCount = 0
		self._workers      = [Worker() for _ in range(capacity)]

	def nextAvailableWorker( self ):
		for _ in self._workers:
			if _.isAvailable:
				return _
		return None

	def submit(self, job, block=False):
		"""Submits a job to the pool. This will create a worker and
		assign the job to the worker."""
		if self.canAdd() or block:
			# NOTE: This is only blocking if canAdd is False
			self._semaphore.acquire()
			self._activeCount += 1
			worker = self.nextAvailableWorker().setJob(job, self._onWorkerJobEnd)
			self._workers.append(worker)
			return worker
		else:
			return None

	def _onWorkerJobEnd( self, worker, job ):
		"""Callback called when a workers's job has ended (wether with a
		success or failure)"""
		self._semaphore.release()
		self._activeCount -= 1

	def canAdd(self):
		"""Tells if a new worker can be started in this pool"""
		return self.count() < self.capacity

	def count(self):
		"""Returns the number of active workers in the queue"""
		return self._activeCount

	def join( self ):
		"""Joins the workers."""
		for _ in self._workers: _.join()
		return self

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
	"""An incident collects failures of the same type and can trigger callbacks
	when a certain amount is reached within a certain period of time"""

	# Keeps only the last 1,000 failures
	MAX_FAILURE          = 1000
	# Keeps only failures that happened during the last week
	MAX_FAILURE_LIFETIME = 60 * 60 * 24 * 7

	# If we have 5 incidents withing 15 minutes, then we trigger the incident
	FAILURE_PERIOD       = 60 * 15
	FAILURE_COUNT        = 5

	@staticmethod
	def GetJobTag( job ):
		"""Returns the signature of the job type"""
		if job:
			return job.__class__.__name__
		else:
			return None

	@staticmethod
	def GetFailureTag( failure ):
		"""Returns the signature of the failure. The signature is what is
		used to group failures, by default it's the class name."""
		if failure:
			return failure.__class__.__name__
		else:
			return None

	def __init__( self, job=None, failure=None ):
		self.jobTag           = Incident.GetJobTag(job)
		self.failureTag       = Incident.GetFailureTag(failure)
		# TODO: The idea of level is how many consecutive periods the incident
		# has with errors.
		self.level            = 0
		self.failures         = []
		self.onAboveThreshold = []

	def matches( self, job, failure ):
		"""Tells if this incident matches the given job and failure"""
		return (self.jobTag     is None or Incident.GetJobTag(job)         == self.jobTag) and \
		        self.failureTag is None or Incident.GetFailureTag(failure) == self.failureTag

	def compact( self ):
		"""Removes the failures that exceed MAX_FAILURE_LIFETIME or MAX_FAILURE"""
		t        = time.time() - self.MAX_FAILURE_LIFETIME
		failures = filter(lambda _:_.happenedAfter(t), self.failures)
		if len(failures) >  self.MAX_FAILURE:
			failures = failures[len(failures) - self.MAX_FAILURE:]
		self.failures = failures
		return self.failures

	def log( self, job, failure ):
		"""Logs the given job and failure to the given incident"""
		if self.matches(job, failure):
			self.failures.append(failure)
			if self.isAboveThreshold():
				self.doAboveThreshold()
			return True
		else:
			return False

	def failuresWithinPeriod( self, period=None ):
		"""Returns the failures that happened within the last period of this
		incident"""
		period = period or self.FAILURE_PERIOD
		t      = time.time() - period
		return filter(lambda _:_.happenedAfter(t), self.failures)

	def isAboveThreshold( self ):
		"""Tells if there was more than `self.FAILURE_COUNT` during the
		last `self.FAILURE_PERIOD` seconds."""
		return len(self.failuresWithinPeriod()) > self.FAILURE_COUNT

	def doAboveThreshold( self ):
		for callback in self.onAboveThreshold:
			try:
				callback(self)
			except Exception, e:
				logging.error("Exception in incident's callback: %s" % (e))

	def __str__( self ):
		return "%s:%s" % (self.jobTag, self.failureTag)

# -----------------------------------------------------------------------------
#
# QUEUE
#
# -----------------------------------------------------------------------------

class Queue:
	"""The queue is the main interfact you use to submit, query and manipulate
	jobs."""

	# FIXME: Add support for that
	MAX_JOBS       = 128000
	# The queue is cleaned up after a given period in seconds
	CLEANUP_PERIOD = 60 * 5

	def __init__( self ):
		self.pool          = Pool()
		self.incidents     = []
		self._runningJobs  = []
		self._lastSelected = -1
		self.lastClean     = -1

	def setPool( self, pool ):
		"""Sets the job pool to be used"""
		# NOTE: Shouldn't we do something with the existing pool if jobs
		# are currently running?
		self.pool = pool

	def clean( self ):
		"""Executes periodic cleaning up operations on the queue"""
		# FIXME: WHat should be done here?
		self.lastClean = time.time()

	def clear( self ):
		"""Clears all the jobs and incidents from the queue"""
		for job in self.list():
			self.remove(job)
		self.incidents = []

	def submit( self, job ):
		"""Submit a new job or a list of jobs in this queue"""
		if type(job) in (tuple, list):
			return map(self.submit, job)
		else:
			if self.has(job):
				return job
			else:
				job.submitted = timestamp ()
				return self._submitJob(job, job.setStatus(JOB_SUBMITTED if job.retries == 0 else JOB_RESUBMITTED))
	def process( self, jobOrID):
		"""Processes the given job. This will mark it as in process and
		run it"""
		job = self._job(jobOrID)
		self._processJob(job, job.setStatus(JOB_IN_PROCESS))
		return self._runJob(job)

	def _runJob( self, job ):
		# Makes sure it's time to execute it
		# ...if not, we return the time we have to wait up until the next event
		# Makes sure the pool can process the event
		# ...if not, we return the maximum time in which the pool will be free/or a callback to when the pool will be free
		self._runningJobs.append(job)
		# STEP 1: We try to run the job and see if it raises a result
		if not job.isUnresolved:
			if not self.pool:
				raise Exception("Workqueue has no associated worker pool (see `setPool`)")
			worker = self.pool.submit(job, block=True)
			# Now we have the worker and we ask it to run the job
			logging.info("{0} IN PROCESS {1}".format(self.__class__.__name__,  job))
			try:
				result = worker.run()
			except Exception, e:
				logging.error("{0} EXCEPTION while running job {1}: {2}".format(self.__class__.__name__, job, e))
				result = Failure(str(e), value=e, job=job)
		else:
			logging.error("{0} UNRESOLVED {1}".format(self.__class__.__name__, job))
			result = Failure("Unresolved job")
		if  result.isSuccess():
			logging.info("{0} SUCCESS   {1}:{2}".format(self.__class__.__name__, job, result))
			job.retries = 0
			# the job is successfully processed
			self._onJobSucceeded(job, result)
			self._runningJobs.remove(job)
		elif result.isFailure():
			logging.warning("{0} FAILURE   {1}:{2}".format(self.__class__.__name__, job, result))
			self._onJobFailed(job, result)
			self._runningJobs.remove(job)
		elif result.isPending():
			logging.info("{0} PENDING   {1}:{2}".format(self.__class__.__name__, job, result))
			assert isinstance(result, Future)
			result.onset(lambda _:self._onJobFinished(job,_))
		else:
			logging.warning("{0} FAILURE   {1}:{2}".format(self.__class__.__name__, job, result))
			self._onJobFailed(job, UnexpectedResult(result))
		return result

	def resubmit( self, job ):
		assert job.id != None, "You cannot resumbit a job without an id: %s" % (job)
		# We increase the number of retries in the job
		self._resubmitJob(job, job.setStatus(JOB_RESUBMITTED))
		assert job.status == JOB_RESUBMITTED
		assert self.get(job.id).retries == job.retries, "{0}.retries {1} != {2}".format(job.id, self.get(job.id).retries, job.retries)
		assert self.get(job.id).status  == job.status, "{0}.status {1} != {2}".format(job.id,   self.get(job.id).status, job.status)
		return job

	def failure( self, job ):
		"""Removes the job from the queue after too many failures"""
		assert job.id != None
		self._failedJob(job, job.setStatus(JOB_FAILED))
		return job

	def complete( self, jobOrJobID ):
		"""Removes the job from the queue"""
		job = self._getJob(jobOrJobID)
		assert job.id != None
		self._completeJob(job, job.setStatus(JOB_COMPLETED))
		return job

	def remove( self, job ):
		"""Removes the job from the queue"""
		job = self._job(job)
		self._removeJob(job, job.setStatus(JOB_REMOVED))
		return job

	def list( self, until=None, since=None, status=None, queue=None ):
		"""Lists the jobs ids in the job queue, by ascending chronological order"""
		return  self._listJobs()

	def getRunningJobs( self ):
		"""Returns the list of jobs that are currently running (aka. `IN_PROCESS`)"""
		return self.list(status=JOB_IN_PROCESS)

	def get( self, jobID ):
		"""Returns the Job instance with the given ID"""
		return self._getJob(jobID)

	def has( self, jobID ):
		"""Tells if this queue has the given job."""
		return self.get(jobID) != None

	def isEmpty( self ):
		"""Tells if the queue is empty or not."""
		return not self._hasJobs()

	def count( self, status=None ):
		"""Tells how many jobs are pending/incoming"""
		raise NotImplementedError

	def join( self ):
		"""Waits until all the queue's workers are done."""
		self.pool.join()
		return self

	def iterate( self, count=-1 ):
		while (count == -1 or count > 0) and self._hasJobs():
			# Takes the next available job
			job = self._getNextJob()
			logging.info("{0} SUBMIT     {1}".format(self.__class__.__name__, job))
			result = self.process(job)
			if count > 0: count -= 1
			# Takes care of cleaning up the queue if it's necessary
			now = time.time()
			if self.lastClean == -1:
				self.lastClean = now
			elif (self.lastClean - now) > self.CLEANUP_PERIOD:
				self.clean()
				self.lastClean = now
			# And finally returns the result
			yield result

	def poll( self, count=1 ):
		return list(self.iterate(count))

	def run( self, count=-1 ):
		"""Runs the workqueue for `count` interations, stopping when
		no more job is available."""
		iteration = 0
		for result in self.iterate(count):
			iteration += 1
		return iteration

	def _onJobFinished( self, jobOrJobID, result):
		if result.isSuccess():
			return self._onJobSucceeded(jobOrJobID, result)
		else:
			return self._onJobFailed(jobOrJobID, result)

	def _onJobSucceeded( self, jobOrJobID, result ):
		job = self._job(jobOrJobID)
		self.complete(job)

	def _onJobFailed( self, job, failure ):
		"""Called when a job has failed."""
		incident     = self._getIncident(job, failure)
		failure.job  = job
		# We increment the number of retries
		job.retries += 1
		if incident.isAboveThreshold():
			# If the incident is above threshold, we won't retry the job,
			# as it's likely to fail again
			logging.warning("{0} !INCIDENT {1}:{2} with {3}".format(self.__class__.__name__, job.id, incident, failure))
			self.failure(job)
		elif job.canRetry():
			# If the incident is not above threshold, and the job has not
			# reached its retry count, we resubmit it
			logging.warning("{0} !RESUBMIT {1}/{2} because of {3}".format(self.__class__.__name__, job.id, job.retries, failure))
			self.resubmit(job)
		else:
			# Otherwise we remove the job from the queue
			logging.warning("{0} !MAXRETRY {1}/{2} after {3}".format(self.__class__.__name__, job.id, job.retries, failure))
			self.failure(job)
		return incident

	def _getIncident( self, job, failure ):
		"""Returns and incident that matches the job and failure"""
		assert failure.isFailure(),"A Failure instance is expected, got {0}".format(failure)
		for incident in self.incidents:
			if incident.log(job, failure):
				return incident
		incident = Incident(job,failure)
		self.incidents.append(incident)
		return incident

	# JOB-ID CONVERSIONS

	def _id( self, jobOrJobID ):
		if isinstance(jobOrJobID, Job):
			res = jobOrJobID.id
			return res
		else:
			return jobOrJobID

	def _job( self, jobOrJobID ):
		if isinstance(jobOrJobID, Job):
			return jobOrJobID
		else:
			return self._getJob(jobOrJobID)

	# BACK-END SPECIFIC METHODS

	def _getNextJob( self ):
		"""Returns the next job and sets it as selected in this queue"""
		raise NotImplementedError

	def _getJob( self, jobID ):
		raise NotImplementedError
		"""Returns the job given its job id."""

	def _hasJobs( self ):
		"""Tells if there are still jobs submitted in the queue"""
		raise NotImplementedError

	def _listJobs( self, status=None ):
		"""Lists the jobs available with the given status (or all otherwise)"""
		raise NotImplementedError

	def _submitJob( self, job, previousStatus ):
		"""Adds a new job to the queue and returns its ID (assigned by the queue)"""
		raise NotImplementedError

	def _processJob( self, job, previousStatus ):
		"""A job is being processed."""
		raise NotImplementedError

	def _resubmitJob( self, job, previousStatus ):
		"""Adds an existing job to the queue and returns its ID (assigned by the queue)"""
		raise NotImplementedError

	def _completeJob( self, job, previousStatus ):
		raise NotImplementedError

	def _failedJob( self, job, previousStatus ):
		"""A job that has failed is archived and might be re-run later."""
		raise NotImplementedError

	def _removeJob( self, job, previousStatus ):
		"""Removes a job from the queue, permanently"""
		raise NotImplementedError


# -----------------------------------------------------------------------------
#
# MEMORY QUEUE
#
# -----------------------------------------------------------------------------

class MemoryQueue(Queue):

	def __init__( self ):
		Queue.__init__(self)
		self.jobs = []

	def _getNextJob( self ):
		"""Returns the next job and sets it as selected in this queue"""
		if self.jobs:
			for _ in self.jobs:
				if not _.isInProcess():
					return _
		else:
			return None

	def _hasJobs( self ):
		"""Tells if there are still jobs submitted in the queue"""
		return self._getNextJob() and True or False

	def _listJobs( self, status=None ):
		return self.jobs

	def _removeJob( self, job, status=None ):
		self.jobs.remove(self._getJob(job))
		return self

	def _getJob( self, jobID ):
		jobID = self._id(jobID)
		for job in self.jobs:
			if job.id == jobID:
				return job
		return None

	def _submitJob( self, job, previousStatus ):
		"""Adds a new job to the queue and returns its ID (assigned by the queue)"""
		# FIXME: Should be synchronized
		job = self._job(job)
		if job not in self.jobs:
			self.jobs.append(job)
			job.setID("%s@%s" % (len(self.jobs) - 1, time.time()))
		return job

	def _resubmitJob( self, job, previousStatus ):
		"""Adds an existing job to the queue and returns its ID (assigned by the queue)"""
		job = self._job(job)
		assert job in self.jobs, "Resubmitting a job that was not previously submitted"
		return job

	def _processJob( self, job, previousStatus ):
		job = self._job(job)
		assert job in self.jobs, "Job to process was not previously submitted"
		return job

	def _completeJob( self, job, previousStatus ):
		# NOTE: This is pretty simple, but should be optimized for big queues
		self.jobs.remove(self._job(job))

	def _failedJob( self, job, previousStatus ):
		# FIXME: A different strategy might be best
		self.jobs.remove(self._job(job))

# -----------------------------------------------------------------------------
#
# DIRECTORY QUEUE
#
# -----------------------------------------------------------------------------

class DirectoryQueue(Queue):

	# FIXME: Should use systems's directory watching

	SUFFIX      = ".json"
	QUEUES      = {
		JOB_SUBMITTED    : "submitted",
		JOB_RESUBMITTED  : "resubmitted",
		JOB_IN_PROCESS   : "inprocess",
		JOB_COMPLETED    : "completed",
		JOB_FAILED       : "failed",
		JOB_REMOVED      : "removed",
	}

	def __init__( self, path ):
		Queue.__init__(self)
		self.path = path
		# We create the directory if it does not exist
		if not os.path.exists(path):
			os.makedirs(path)
		self.clean()
		# FIXME: Make suret that running jobs that haven't been touched
		# for X hours are put into resubmitted
		# for running in self._listJobs(status=JOB_IN_PROCESS):
		assert len(self.QUEUES.values()) == len(set(self.QUEUES.values())), "DirectoryQueue must have separate directories for each state"
		for queue in self.QUEUES.values():
			queue_path = path + "/" + queue
			if not os.path.exists(queue_path):
				os.makedirs(queue_path)

	def clean( self ):
		Queue.clean(self)
		# We look for jobs that are registered in more than one queue
		job_status = {}
		for status in self.QUEUES:
			for job in self._listJobs(status):
				job_status.setdefault(job, [])
				job_status[job].append(status)
		for job, status in job_status.items():
			if len(status) > 2:
				# For the jobs registered in more than one queue, we want to
				# keep only one file of the job. The rationale is that if it's
				# still in submitted, then it might have failed, so we'll keep
				# the submitted. The risk is doing it twice, but it's better
				# than not doing it.
				keep = None
				if   JOB_COMPLETED   in status: keep = JOB_COMPLETED
				elif JOB_SUBMITTED   in status: keep = JOB_SUBMITTED
				elif JOB_RESUBMITTED in status: keep = JOB_RESUBMITTED
				elif JOB_IN_PROCESS  in status: keep = JOB_IN_PROCESS
				elif JOB_FAILED      in status: keep = JOB_FAILED
				elif JOB_REMOVED     in status: keep = JOB_REMOVED
				status.remove(keep)
				for _ in status:
					logging.warning("Removing duplicate job file {0}/{1} {2}".format(_, status, self._getPath(job, _)))
					self._removeJobFile(job, _)

	def read( self, path, sync=True ):
		"""Atomically read the file at the given path"""
		flags = os.O_RDONLY
		if sync: flags = flags | os.O_RSYNC
		fd    = os.open(path, flags)
		data  = None
		try:
			last_read = 1
			data      = []
			while last_read > 0:
				t = os.read(fd, 128000)
				data.append(t)
				last_read = len(t)
			data = "".join(data)
			os.close(fd)
		except StandardError, e:
			os.close(fd)
			raise e
		return data

	def write( self, data, path, sync=True, append=False ):
		"""Atomically write the given data in the file at the given path"""
		flags = os.O_WRONLY | os.O_CREAT
		if sync:       flags = flags | os.O_DSYNC
		if not append: flags = flags | os.O_TRUNC
		fd    = os.open(path, flags)
		try:
			os.write(fd, data)
			os.close(fd)
		except StandardError, e:
			os.close(fd)
			raise e
		return self

	def timestamp( self ):
		now = datetime.datetime.now()
		return "%04d%02d%02dT%02d:%02d%02d%d" % (
			now.year, now.month, now.day, now.hour, now.minute, now.second, now.microsecond
		)

	def _listJobs( self, status=JOB_SUBMITTED ):
		path = self.path + "/" + self.QUEUES[status]
		if os.path.exists(path):
			for _ in os.listdir(path):
				if _.endswith(self.SUFFIX):
					yield _[:-len(self.SUFFIX)]

	def _getNextJob( self, status=JOB_SUBMITTED ):
		"""Returns the next job and sets it as selected in this queue"""
		iterator = self._listJobs(status)
		job      = iterator.next()
		return self._getJob(job)

	def _hasJobs( self, status=JOB_SUBMITTED ):
		"""Tells if there are still jobs submitted in the queue"""
		# NOTE: JOB_SUBMITTED and JOB_RESUBMITTED are the same keys, so it's
		# OK to do just that
		iterator = self._listJobs(status)
		try:
			iterator.next()
			return True
		except StopIteration, e:
			return False

	def _submitJob( self, job, previousStatus ):
		"""Adds a new job to the queue and returns its ID (assigned by the queue)"""
		assert job.id is None
		new_id = self.timestamp() + "-" + job.__class__.__name__
		job.setID(new_id)
		self._writeJob(job)
		return job

	def _processJob( self, job, previousStatus ):
		self._removeJobFile(job, previousStatus)
		self._writeJob(job)
		return job

	def _resubmitJob( self, job, previousStatus ):
		"""Adds an existing job to the queue and returns its ID (assigned by the queue)"""
		self._removeJobFile(job, previousStatus)
		self._writeJob(job)
		return job

	def _completeJob( self, job, previousStatus ):
		self._removeJobFile(job, previousStatus)
		self._writeJob(job)
		return job

	def _failedJob( self, job, previousStatus ):
		self._removeJobFile(job, previousStatus)
		assert job.status == JOB_FAILED
		self._writeJob(job)
		return job

	def _removeJob( self, job, previousStatus=None ):
		# NOTE: This method is kept as it's used by the queue
		job = self._getJob(job)
		if job:
			self._removeJobFile(job, previousStatus or job.status)
		else:
			logging.debug("DirectoryQueue job already removed {0}".format(job))

	def _writeJob( self, job ):
		path = self._getPath(job)
		if path:
			logging.debug("DirectoryQueue save   {0}".format(path[len(self.path):]))
			self.write(asJSON(job.export()), path)
		else:
			logging.debug("DirectoryQueue cannot find path for job {0}".format(job))

	def _removeJobFile( self, job, status=None ):
		"""Physically removes the job file."""
		path = self._getPath(job, status)
		logging.debug("DirectoryQueue remove {0}".format(path[len(self.path):]))
		if os.path.exists(path): os.unlink(path)

	def _getPath( self, job, status=None ):
		job_id = None
		queue  = None
		if not job:
			return None
		if status:
			if isinstance(job, Job):
				job_id = job.id
				assert job_id, "Job has no id, so it cannot be saved: {0}".format(job)
			else:
				job_id = job
				assert job_id, "No job or job id given"
			queue  = self.QUEUES[status]
		else:
			assert isinstance(job, Job), "Queue can only be omitted when the given job is an instance (not a job id)"
			if job.status is None:
				# If the job has no status, then we retrieve the job through
				# the _getJob (which will override the job's declared status)
				existing_job = self._getJob(job.id)
				if existing_job: job.status = existing_job.status
			# If there's a job status, we can assign a queue
			if job.status:
				assert job.status is not None, "Job has no status: {0}".format(job)
				queue  = self.QUEUES[job.status]
			job_id = job.id
		if queue and job_id is not None:
			return self.path + "/" + queue + "/" + job_id + self.SUFFIX
		else:
			# A job without status or id cannot be saved
			return None

	def _getJob( self, jobOrJobID ):
		"""Returns the job given its job id."""
		path       = None
		job_id     = None
		job_status = None
		if isinstance(jobOrJobID, Job):
			job_id = jobOrJobID.id
		else:
			job_id = jobOrJobID
		# We look for the different queues, and see if there's a job with the
		# given job id (hopefull, there's only one)
		matching_path = []
		# FIXME: This is not very fast, but at least it is self-healing. For
		# high throughput, this might have to be revisited
		for status in self.QUEUES:
			_ = self._getPath(job_id, status)
			if _ and os.path.exists(_):
				job_status = status
				if _ not in matching_path:
					matching_path.append(_)
		assert (len(matching_path) <= 1), "Job found in multiple places: {0} in {1}".format(job_id, matching_path)
		if len(matching_path) >= 1:
			path = matching_path[0]
		# If the path exists, we load the job and return it
		if path and os.path.exists(path):
			logging.debug("DirectoryQueue read   {0}".format(path[len(self.path):]))
			try:
				job = json.loads(self.read(path))
			except ValueError, e:
				# FIXME: We should do something with that!
				# If we're here, we can't decode the JSON properly
				logging.error("Corrupt job file: " + str(path))
				return None
			job = Job.Import(job, job_id)
			# NOTE: We override the job status contained in the JSON
			# with its actual location
			if job_status is not None:
				job.status = job_status
			# JOB_SUBMITTED and JOB_RESUBMITTED are in the same physical
			# directory, so we disambiguate using retries
			if job.status == JOB_SUBMITTED and job.retries > 0:
				job.status = JOB_RESUBMITTED
			return job
		else:
			# Otherwise the job cannot be found, and then does not exist (anymore)
			return None

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

# -----------------------------------------------------------------------------
#
# DAEMON
#
# -----------------------------------------------------------------------------

class Daemon:
	"""A very simple class that wraps a queue and runs it, sleep and waking
	up every second to check for more work. This daemon will take over the
	main loop."""

	def __init__( self, queue=None, period=1 ):
		self.queue       = queue
		self.isRunning   = False
		self.sleepPeriod = period
		self.signalsRegistered = False

	def _registerSignals( self ):
		if not self.signalsRegistered:
			signals = ['SIGINT',  'SIGHUP', 'SIGABRT', 'SIGQUIT', 'SIGTERM']
			for sig in signals:
				try:
					signal.signal(getattr(signal,sig), self.onSignal)
				except Exception, e:
					logging.error("Cannot register signals: {0}".format(e))

	def run( self ):
		self._registerSignals()
		self.isRunning = True
		while self.isRunning:
			if self.queue.run(1) == 0 and self.isRunning:
				t = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())
				logging.info("{0} - No job left, sleeping for {1}s".format(t, self.sleepPeriod))
				time.sleep(self.sleepPeriod)

	def stop( self ):
		self.isRunning = False

	def onSignal( self, a=None, b=None, c=None ):
		self.stop()
		map(self.queue.resubmit, self.queue.getRunningJobs())
		sys.exit(0)

# EOF - vim: tw=80 ts=4 sw=4 noet
