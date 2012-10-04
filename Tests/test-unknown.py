import unittest
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

class UnkownJob(unittest.TestCase):

	def setUp( self ):
		self.queue  = DirectoryQueue(__file__.split(".")[0])
		self.queue.clear()
		self.queue.submit(Job.Import(UNKNOWN_JOB))

	def testTransparency( self ):
		job = Job.Import(UNKNOWN_JOB)
		self.assertTrue( compare(UNKNOWN_JOB, job.export()))
		# FIXME: Test transparency through the lifecycle

if __name__ == "__main__":
	unittest.main()

# EOF
