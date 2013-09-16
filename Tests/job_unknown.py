import unittest, shutil
from   workqueues  import *

UNKNOWN_JOB = {
	"data": {
		"a": {
			"audio": {
				"bitrate": None,
				"codec": "Vorbis"
			},
			"container": {
				"format": "Matroska",
				"name": "WebM"
			},
			"dimension": None,
			"duration": 78.4,
			"format": "webm",
			"language": None,
			"original": "Data/Video/884cd7a8-3ca9-4e3f-9adc-5020568f30c6/data",
			"rid": "884cd7a8-3ca9-4e3f-9adc-5020568f30c6",
			"subtitles": [],
			"timestamp": 20120920013807,
			"type": "artnet.model.Video",
			"video": {
				"bitrate": "200000",
				"codec": "On2 VP8"
			}
		}
	},
	"frequency": None, "repeat": -1, "result": None, "scheduled": -1, "timeout": -1, "type": "artnet.operations.VideoPreview", "until": -1
}

NOCLASS_JOB = {
	"data": {},
	"frequency": None, "repeat": -1, "result": None, "scheduled": -1, "timeout": -1, "type": "", "until": -1
}

def compare( a, b ):
	assert type(a) == type(b)
	if type(a) is dict:
		return compare(a.items(), b.items())
	elif type(a) in (list, tuple):
		if len(a) != len(b):
			return False
		for i in range(len(a)):
			if not compare(a[i], b[i]):
				return False
		return True
	else:
		return a == b

assert compare(UNKNOWN_JOB, UNKNOWN_JOB)

class BrokenJobs(unittest.TestCase):

	def setUp( self ):
		self.queue  = DirectoryQueue(__file__.split(".")[0])
		self.queue.clear()

	def tearDown( self ):
		pass
		# self.queue.clear()
		# shutil.rmtree(self.queue.path)

	# def testTransparency( self ):
	# 	"""Makes sure that there's not data loss between import and export"""
	# 	job = Job.Import(UNKNOWN_JOB)
	# 	self.assertTrue( compare(UNKNOWN_JOB, job.export()))
	# 	job = Job.Import(NOCLASS_JOB)
	# 	self.assertTrue( compare(NOCLASS_JOB, job.export()))

	#def testFailUnknownJob( self ):
	#	"""Ensures that an unknown job will fail."""
	#	job = Job.Import(UNKNOWN_JOB)
	#	self.assertTrue(self.queue.isEmpty())
	#	self.assertEquals(job.retries, 0)
	#	self.queue.submit(job)
	#	result = self.queue.poll(1)[0]
	#	self.assertTrue(isinstance(result, Failure))
	#	# We cannot expect the jobs to have the same references, as
	#	# a workqueue is not an ORM, but we do expect them to be identical
	#	self.assertEquals(result.job.id,       job.id)
	#	self.assertEquals(result.job.export(), job.export())

	def testRetries( self ):
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




if __name__ == "__main__":
	unittest.main()

# EOF
