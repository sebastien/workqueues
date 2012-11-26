import unittest
from   workqueues  import *

class JobThatFails(Job):

	def run( self ):
		raise Exception("Failure")


class JobFailure(unittest.TestCase):

	def setUp( self ):
		# We need to register the job
		Job.Register(JobThatFails)
		self.queue  = DirectoryQueue(__file__.split(".")[0])
		self.queue.clear()
		job = JobThatFails()
		self.queue.submit(job)
		self.job_id = job.id

	def testDirectJobFailure( self ):
		"""We test the job failure when we're using the same directory queue
		instance."""
		self.assertIsNotNone(self.queue.get(self.job_id))
		self.assertEquals(self.queue.get(self.job_id).retries, 0)
		for retries in range(5):
			self.assertEquals(self.queue.get(self.job_id).retries, retries)
			self.assertEquals(self.queue.get(self.job_id).status,  JOB_SUBMITTED if retries==0 else JOB_RESUBMITTED)
			self.queue.run(1)
		self.queue.run(1)
		self.assertEquals(self.queue.get(self.job_id).status,  JOB_FAILED)
		self.assertEquals(self.queue.run(1), 0)

	def testIndirectJobFailure( self ):
		"""We test the job failure when we're using a new directory queue each
		time. This checks that the job persistence preserves the retry attribute
		properly."""
		queue = DirectoryQueue(__file__.split(".")[0])
		self.assertIsNotNone(queue.get(self.job_id))
		self.assertEquals(queue.get(self.job_id).retries, 0)
		for retries in range(5):
			queue = DirectoryQueue(__file__.split(".")[0])
			self.assertEquals(queue.get(self.job_id).retries, retries)
			self.assertEquals(queue.get(self.job_id).status,  JOB_SUBMITTED if retries==0 else JOB_RESUBMITTED)
			self.assertEquals(queue.run(1), 1)
		queue = DirectoryQueue(__file__.split(".")[0])
		queue.run(1)
		self.assertEquals(queue.get(self.job_id).status,  JOB_FAILED)
		self.assertEquals(queue.run(1), 0)

if __name__ == "__main__":
	unittest.main()

# EOF
