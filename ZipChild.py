### Python Developer: Sulaiha Subi ###
### Version 1.5: 23rd June 2021 (Testing)###
### Python Version: 3.7 ###


#### Import Library Here: ####
import os
import zipfile
# Making Connection Mongodb
import namelist as namelist
import requests
from numpy import info
from pip._internal.utils import logging
from pymongo import MongoClient
import re
from io import BytesIO

myclient = MongoClient("mongodb://localhost:27017/")
# Database
db = myclient["RiseHill"]
# Created or Switched to collection
Collection = db["FilesCollection3"]

#Get the URL
url = 'http://rdatests3.s3.ap-southeast-1.amazonaws.com/OneDrive_1_05-05-2021.zip'
# url = 'https://rdatests3.s3.ap-southeast-1.amazonaws.com/OneDrive_1_05-05-2021+2.zip'
content = requests.get(url)

from io import BytesIO, StringIO
from zipfile import ZipFile

# After getting request, read what inside the zip. Listing all the files path inside the Zip Parents
z = zipfile.ZipFile(BytesIO(content.content))

# Get the content from Parent Zip
paths = []
sizes = []
extentions = []

# Get the content from Child Zip
childPaths = []
childSizes = []
childExtentions = []


x = str(z.namelist())
filepaths = x.split(",")

# This Function will check the zip child and then read it, after that continue read the other files inside the zip
# namelist= datatype (string)
# infolist= object (anydatatype)

# function to retrieve zip child
with z as zfile:
    for info in zfile.infolist():
        file_path = str(info.filename)
        paths.append(file_path)
        # print(file_path)

        # Size Reading: Parent Zip
        file_size = str(info.file_size) + ' bytes'
        sizes.append(file_size)
        # print(file_size)

        # File Extension: Parent Zip
        tup1 = os.path.splitext(file_path)
        file_extension = tup1[1]
        extentions.append(file_extension)
        # print(file_extension)

        # To print all inside the Zip Parent (Files)
        # print(file_path, file_size, file_extension)


        # If found zip child inside the Parent, go to this process
        # re.search will find all the possible child zip and then read the files inside
        if re.search(r'\.zip$', file_path) != None:
            # We have a zip within a zip (reading the child zips)
            # print(name)
            zfiledata = BytesIO(zfile.read(file_path))
            with zipfile.ZipFile(zfiledata) as zfile2:
                for name2 in zfile2.infolist():
                    # child_path = file_path + name2.filename
                    child_path = file_path + '/' + name2.filename
                    paths.append(child_path)
                    # print(childPaths)


                    # to print the paths of zip child
                    # print(file_path + '/', name2.filename)

                    # to print size of the zip child(s)
                    child_size = str(name2.file_size) + ' bytes'
                    sizes.append(child_size)
                    # print(child_size)

                    # to print extentions of the zip child(s)
                    tup1 = os.path.splitext(name2.filename)
                    zipchild_extension = tup1[1]
                    extentions.append(zipchild_extension)
                    # print(zipchild_extension)

                    # to print all about the zip child (s)
                    # print(file_path + '/', name2.filename, child_size, zipchild_extension)
                    # print(child_path, child_size, zipchild_extension)
data = []
mainData = {} #creating the dictionary to send inside the mongodb
for i in range(len(paths)):
        path = paths[i]
        size = sizes[i]
        extension = extentions[i]
        # childPath = childPaths[i]
        # childSize = childSizes[i]
        # childExtention = childExtentions[i]
        file = {"path": path,
        "extension" : extension,
         "size" : size
        # "Zip Child Path" : childPath,
        # "Zip Child Size": childSize,
        # "Zip Child Extension": childExtention
        }
        data.append(file)

for i in range(len(data)):
    mainData['File' + str(i+1)] = data[i]
Collection.insert_one(mainData)