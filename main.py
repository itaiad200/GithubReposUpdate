import os
import sys
import csv

repo_properties = ["repo_id","repo_name","repo_url","actor_id","actor_login","actor_avatar_url","actor_url","org_id","org_login","org_url","commits","html_url","forked","description","full_name","lang","watchers","forks_count"]
def extractRepos(filePath):
    repos = []
    with open(filePath) as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in spamreader:
            if(len(row) != len(repo_properties)):
                print "Warning: line format is wrong and will be skipped. Length: " + str(len(row)) + ". Row: " + ','.join(row)
                continue
            repos.append(dict(zip(repo_properties, row)))

    return repos

def main(arg):
    if(len(arg) != 1):
        raise ValueError("Exactly 1 file is expected as input")

    repos = extractRepos(arg[0])

    







if __name__ == '__main__':
    main(sys.argv[1:])