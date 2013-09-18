import unittest, shutil
from   workqueues  import *

UNKNOWN_JOB = {
	"data": {},
	"frequency": None,
	"repeat": -1,
	"result": None,
	"scheduled": -1,
	"timeout": -1,
	"type": "some.JobThatDoesNotExist",
	"until": -1
}

class SampleJob(Job):

	def run( self ):
		pass

class FailingJob(Job):

	def run( self ):
		assert False, "This job will always fail"

class BackendTest(unittest.TestCase):

	def setUp( self ):
		Job.Register(SampleJob)
		Job.Register(FailingJob)
		self.queue = MemoryQueue()

	def testClean( self ):
		"""Justs makes sure that the clean method is implemented/supported"""
		self.queue.clean()

	def testClear( self ):
		self.assertTrue(self.queue.isEmpty())
		self.queue.submit(SampleJob())
		self.assertFalse(self.queue.isEmpty())
		self.queue.clear()
		self.assertTrue(self.queue.isEmpty())

	def testSubmit( self ):
		job = SampleJob()
		self.assertEquals( job.status, None )
		self.assertIsNone( job.id )
		self.assertFalse( self.queue.has(job) )
		self.assertFalse( self.queue.has(job.id) )
		# We ensure that the returned job is the same
		job_ref = self.queue.submit(job)
		self.assertIsNotNone( job_ref.id )
		self.assertTrue( self.queue.has(job_ref) )
		self.assertTrue( self.queue.has(job_ref.id) )
		self.assertEquals( job.export(), job_ref.export() )
		self.assertEquals( job_ref.status, JOB_SUBMITTED )
		self.assertEquals( id(job), id(job_ref))
		# We retrieve a copy of the job and make sure it's equal. It might
		# be the same physical instance, but it's not guaranteed.
		job_copy = self.queue.get(job.id)
		self.assertEquals( job_copy.status, JOB_SUBMITTED )
		self.assertEquals( job_copy.export(), job.export() )
		# Submitting a job twice should not do anything to the job, as
		# it was already submitted
		job = self.queue.submit(job)
		self.assertEquals( job.status, JOB_SUBMITTED )
		self.assertEquals( job.export(), job_ref.export() )

	def testResubmit( self ):
		job = FailingJob()
		job = self.queue.submit(job)
		self.assertEquals( job.status, JOB_SUBMITTED )
		# We manually resubmit the job
		job_ref = self.queue.resubmit(job)
		self.assertEquals( job_ref.status, JOB_RESUBMITTED )
		self.assertEquals( id(job), id(job_ref))
		# We retrieve the job, which will return a copy, that we want
		# to make sure is identical
		job_copy = self.queue.get(job_ref.id)
		self.assertEquals( job.export(), job_copy.export() )

	def testFailure( self ):
		result = None
		# We submit the job
		job    = self.queue.submit(FailingJob())
		self.assertEquals( job.status,  JOB_SUBMITTED )
		self.assertEquals( job.retries, 0 )
		# Poll the queue, which will run the job, which will the fail
		result = self.queue.poll(1)[0]
		self.assertTrue( isinstance(result, Result))
		self.assertTrue( isinstance(result, Failure))
		self.assertEquals( result.job.id,      job.id )
		self.assertEquals( result.job.retries, 0 )
		self.assertEquals( result.job.status,  JOB_FAILED )
		# We resubmit the failed job
		job  = self.queue.submit(result.job)
		self.assertEquals( job.status,  JOB_RESUBMITTED )
		self.assertEquals( job.retries, 1)

	def testDirectJobFailure( self ):
		"""We test the job failure when we're using the same directory queue
		instance."""
		# We create the job and submit it
		job = FailingJob()
		self.assertEquals(job.retries, 0)
		job_ref = self.queue.submit(job)
		self.assertEquals(job.retries,     0)
		self.assertEquals(job_ref.retries, 0)
		self.assertIsNotNone(self.queue.get(job.id))
		self.assertEquals(self.queue.get(job.id).retries, 0)
		# We run the queue, and the job should be automatically resubmitted
		for retries in range(5):
			self.assertEquals(self.queue.get(self.job_id).retries, retries)
			self.assertEquals(self.queue.get(self.job_id).status,  JOB_SUBMITTED if retries==0 else JOB_RESUBMITTED)
			self.queue.run(1)
		self.queue.run(1)
		self.assertEquals(self.queue.get(self.job_id).status, JOB_FAILED)
		self.assertEquals(self.queue.run(1), 0)

	# def testIndirectJobFailure( self ):
	# 	"""We test the job failure when we're using a new directory queue each
	# 	time. This checks that the job persistence preserves the retry attribute
	# 	properly."""
	# 	queue = DirectoryQueue(__file__.split(".")[0])
	# 	self.assertIsNotNone(queue.get(self.job_id))
	# 	self.assertEquals(queue.get(self.job_id).retries, 0)
	# 	for retries in range(5):
	# 		queue = DirectoryQueue(__file__.split(".")[0])
	# 		self.assertEquals(queue.get(self.job_id).retries, retries)
	# 		self.assertEquals(queue.get(self.job_id).status,  JOB_SUBMITTED if retries==0 else JOB_RESUBMITTED)
	# 		self.assertEquals(queue.run(1), 1)
	# 	queue = DirectoryQueue(__file__.split(".")[0])
	# 	queue.run(1)
	# 	self.assertEquals(queue.get(self.job_id).status,  JOB_FAILED)
	# 	self.assertEquals(queue.run(1), 0)

	def testUnknownJob( self ):
		"""Ensures that a failed job has its retry counter increased"""
		job = Job.Import(UNKNOWN_JOB)
		self.assertTrue(self.queue.isEmpty())
		self.assertEquals(job.retries, 0)
		self.queue.submit(job)
		job = self.queue.get(job.id)
		self.assertEquals(job.retries, 0)
		self.assertEquals(job.status,  JOB_SUBMITTED)
		self.assertEquals(Job.RETRIES, 5)
		# NOTE: The iterations are copied/pasted because it's quite useful
		# to know which iteration breaks, if it does.
		# First iteration
		result = self.queue.poll(1)[0]
		self.assertTrue(isinstance(result, Failure))
		self.assertEquals(result.job.retries, 1)
		self.assertEquals(result.job.status,  JOB_RESUBMITTED)
		# Second iteration
		result = self.queue.poll(1)[0]
		self.assertTrue(isinstance(result, Failure))
		self.assertEquals(result.job.status,  JOB_RESUBMITTED)
		self.assertEquals(result.job.retries, 2)
		# Third iteration
		result = self.queue.poll(1)[0]
		self.assertTrue(isinstance(result, Failure))
		self.assertEquals(result.job.status,  JOB_RESUBMITTED)
		self.assertEquals(result.job.retries, 3)
		# Fourth iteration
		result = self.queue.poll(1)[0]
		self.assertTrue(isinstance(result, Failure))
		self.assertEquals(result.job.status,  JOB_RESUBMITTED)
		self.assertEquals(result.job.retries, 4)
		# Fifth (and last) iteration
		result = self.queue.poll(1)[0]
		self.assertTrue(isinstance(result, Failure))
		self.assertEquals(result.job.status,  JOB_FAILED)
		self.assertEquals(result.job.retries, 4)
		self.assertEqual(retries, Job.RETRIES)
		self.assertEquals(result.job.status, JOB_FAILED)
		self.assertEquals(result.job.retries, Job.RETRIES)

	def testFailOnUnknownJob( self ):
		"""Ensures that an unknown job will fail."""
		job = Job.Import(UNKNOWN_JOB)
		self.assertTrue(self.queue.isEmpty())
		self.assertEquals(job.retries, 0)
		self.queue.submit(job)
		result = self.queue.poll(1)[0]
		self.assertTrue(isinstance(result, Failure))
		# We cannot expect the jobs to have the same references, as
		# a workqueue is not an ORM, but we do expect them to be identical
		self.assertEquals(result.job.id,       job.id)
		self.assertEquals(result.job.export(), job.export())

class DirectoryBackendTest(BackendTest):

	def setUp( self ):
		Job.Register(SampleJob)
		self.queue = DirectoryQueue(__file__.split(".")[0])

	def tearDown( self ):
		self.queue.clear()
		shutil.rmtree(self.queue.path)

if __name__ == "__main__":
	unittest.main()

# EOF
