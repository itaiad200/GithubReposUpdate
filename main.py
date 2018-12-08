import time
import sys
import csv
import urllib2
import json
import Queue
from threading import Thread
from threading import Lock

NUMBER_OF_THREADS = 5

repo_properties = ["repo_id","repo_name","repo_url","actor_id","actor_login","actor_avatar_url","actor_url","org_id","org_login","org_url","commits","html_url","forked","description","full_name","lang","watchers","forks_count"]
GithubUrlTemplate = "https://api.github.com/repos/{repo_name}"

QueueLock = Lock()
ResultsLock = Lock()

Results = {}

def ExtractRepos(filePath):
    reposQueue  = Queue.Queue()
    with open(filePath) as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')

        # skipping header
        next(reader)

        for row in reader:
            if(len(row) != len(repo_properties)):
                print "Warning: line format is wrong and will be skipped. Length: " + str(len(row)) + ". Row: " + ','.join(row)
                continue

            reposQueue.put(dict(zip(repo_properties, row)))

    return reposQueue

def WriteRepos():
    with open('output.csv', 'w') as csv_file:
        writer = csv.writer(csv_file)
        for key, value in Results.items():
            writer.writerow([key, value])

def main(arg):
    if(len(arg) != 1):
        raise ValueError("Exactly 1 file is expected as input")

    reposQueue = ExtractRepos(arg[0])
    workers = []
    for i in range(NUMBER_OF_THREADS):
        workers.append(Worker(reposQueue, i))
        workers[i].start()

    for i in range(NUMBER_OF_THREADS):
        workers[i].join()

    WriteRepos()


class Worker(Thread):
    def __init__(self, reposQueue, id):
        Thread.__init__(self)
        self.reposQueue = reposQueue
        self.id = id

    def run(self):
        while not self.reposQueue.empty():
            with QueueLock:
                if not (self.reposQueue.empty()):
                    repo = self.reposQueue.get()
                else:
                    continue

            repoAddress = GithubUrlTemplate.replace("{repo_name}", repo["repo_name"])
            print("Worker %d Working on repo %s", self.id, repoAddress)

            try:
                contents = urllib2.urlopen(repoAddress).read()
                contentSerialized = json.loads(contents)

                with ResultsLock:
                    Results[repo["repo_name"]] = str(contentSerialized["updated_at"])

            except urllib2.HTTPError, e:
                if e.code == 404:
                    print "Repo Not Found: " + repo["repo_name"]
                if e.code == 403:
                    print "Throttled"
                    with QueueLock:
                        # Adding repo back to the queue since result wasn't received
                        self.reposQueue.put(repo)

                    # sleeping until quota reset
                    resetTimeInUtcEpoch = int(e.headers["X-RateLimit-Reset"])
                    sleepTimeInSec = resetTimeInUtcEpoch - int(time.time());
                    print "Thread " + str(self.id) + " sleeps for " + str(sleepTimeInSec) + " seconds"
                    time.sleep(sleepTimeInSec)

if __name__ == '__main__':
    main(sys.argv[1:])