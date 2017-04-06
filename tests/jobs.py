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


class Jobs(unittest.TestCase):

	def testTransparency( self ):
		"""Makes sure that there's not data loss between import and export"""
		job = Job.Import(UNKNOWN_JOB)
		self.assertEquals( UNKNOWN_JOB, job.export())
		job = Job.Import(NOCLASS_JOB)
		self.assertEquals( NOCLASS_JOB, job.export())

	def testJobRetries( self ):
		job = Job()
		job.retries = 0
		self.assertEquals(job.retries, 0)
		self.assertEquals(job.export()["retries"], 0)
		job.retries = 1
		self.assertEquals(job.retries, 1)
		self.assertEquals(job.export()["retries"], 1)

if __name__ == "__main__":
	unittest.main()

# EOF
