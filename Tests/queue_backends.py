import unittest
from   workqueues  import *

class SampleJob(Job):

	def run( self ):
		pass

class FailingJob(Job):

	def run( self ):
		assert False, "This job will always fail"

class BackendTest(unittest.TestCase):

	def setUp( self ):
		Job.Register(SampleJob)
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
		self.assertEquals( job_copy.export(), job.export() )
		self.assertEquals( job_copy.status, JOB_SUBMITTED )
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


if __name__ == "__main__":
	unittest.main()

# EOF
