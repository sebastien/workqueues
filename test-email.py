import json
from   workqueues import *

# email.password is a JSON file like this
# {"smtp":"smtp.server.com", "user":"user@domain.com", "password":"123456"}
EMAIL = json.loads(file("email.passwd").read())

class SendEmail(Job):

	MESSAGE = """\
	|From: ${from}
	|To:   ${to}
	|Subject: ${subject}
	|
	|${message}
	|--
	|Timestamp: ${timestamp}
	|Iteration: ${iteration}
	|Result:    ${result}
	|--
	""".replace("\t|", "")
	DATA = ["to", "subject", "message"]

	def __init__( self, to, subject, message, origin=None, **kwargs ):
		Job.__init__(self, **kwargs)
		self.to      = to
		self.subject = subject
		self.message = message
		self.origin  = origin
		self.host    = EMAIL["smtp"]
	
	def run( self ):
		server  = smtplib.SMTP(self.host)
		origin  = self.origin or "Workqueues workqueues-test@ffctn.com"
		message = string.Template(self.MESSAGE).safe_substitute({
			"from": origin,
			"to":      self.to,
			"subject": self.subject,
			"message": self.message,
		})
		server.ehlo()
		server.starttls()
		server.ehlo()
		if self.password:
			server.login(self.user, self.password)
		server.sendmail(origin, [self.user], message)
		try:
			server.quit()
		except:
			pass
		return message

if __name__ == "__main__":
	# This is is the "client" part, ie. where we submit jobs to the queue
	queue = DirectoryQueue("Queue")
	count = 10
	for i in range(count):
		job = SendEmail(
			"sebastien@ffctn.com",
			"Workqueue test %d/%d",
			"This is a workqueue test message" 
		)
		queue.submit(job)
	# This is is the "server" part, ie. where we set a pool of workers to
	# the queue
	pool  = Pool(5)
	queue.setPool(pool)
	# And we call "process" which will process everything
	queue.run()
# EOF
