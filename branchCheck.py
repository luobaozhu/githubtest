# -*- coding: utf-8 -*-
import hashlib
import os
import socket
import pandas as pd


def getFileMd5(path):
    fd = open(path, "r")
    fcont = fd.read()
    fd.close()
    fmd5 = hashlib.md5(fcont).hexdigest()
    return fmd5


def getMd5DataFrame(directoryType):
    directory = "/data/deployments/%s/sparkJobs" % directoryType
    data = []
    columns = ["file_directory", "file_md5_" + directoryType]
    for root, dirs, files in os.walk(directory):
        # 略过隐藏文件（夹）及非py文件
        files = [f for f in files if f[0] != '.' and f.endswith(".py")]
        dirs[:] = [d for d in dirs if d[0] != '.']
        for fileName in files:
            fileDirectory = os.path.join(root, fileName)
            fileMd5 = getFileMd5(fileDirectory)
            fileDirectory = fileDirectory.replace(directory, "")
            data.append([fileDirectory, fileMd5])
    df = pd.DataFrame(data=data, columns=columns)
    return df


host = socket.gethostname()
devDf = getMd5DataFrame("dev")
prodDf = getMd5DataFrame("prod")
mergeDf = devDf.merge(prodDf, how="left", on="file_directory")
inconsistent = []
for row in mergeDf.values.tolist():
    if row[1] != row[2]:
        inconsistent.append(row[0])
if inconsistent:
    print("%s has inconsistent files:" % host)
    for i in inconsistent:
        print(i)
    exit(1)
else:
    print("All files are consistent on %s." % host)