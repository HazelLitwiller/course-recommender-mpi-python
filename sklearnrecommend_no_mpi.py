import pandas as pd
import numpy as np
import random
from sklearn.neighbors import NearestNeighbors
import time


def preprocess(major, groupToDepartment, f20, s21):

    newCols = ["String","SameGroup","SameDepartment","Level","Avg Student GPA","Total Grades"]
    df = pd.DataFrame([],columns=newCols)
    courses = []
    currIndex = 0
    for x in [f20.iterrows(),s21.iterrows()]:
        for index,row in x:
            currCourseStr = str(row["Course Subject"])+str(row["Catalog Number"])
            sameGroup = False
    
            for v in groupToDepartment.values():
                if major in v and row["Department Code"] in v:
                    sameGroup = True
            
            if currCourseStr not in courses:
                courses.append(currCourseStr)
                currData = [str(row["Course Subject"])+str(row["Catalog Number"]),
                            1 if sameGroup else 0,
                            1 if major==row["Department Code"] else 0,
                            str(row["Catalog Number"])[0],
                            row["Avg Student GPA"],
                            row["Total Grades"]]
                        
                currDF = pd.DataFrame([currData],columns=newCols,index=[currIndex])
                df = df.append(currDF)
                currIndex += 1
            else:
                tempIndex = df.index[df["String"] == currCourseStr].tolist()[0]
                existingRow = df.loc[tempIndex]
                existingGPA = 0 if existingRow["Avg Student GPA"] == "NR" else float(existingRow["Avg Student GPA"])
                currGPA = 0 if row["Avg Student GPA"] == "NR" else float(row["Avg Student GPA"])
                existingTotal = 0 if existingRow["Total Grades"] == "NR" else int(existingRow["Total Grades"])
                currTotal = 0 if row["Total Grades"] == "NR" else int(row["Total Grades"])

                if existingTotal + currTotal > 0:
                    df.loc[tempIndex,"Avg Student GPA"] = (currTotal/(existingTotal+currTotal))*currGPA + (existingTotal/(existingTotal+currTotal))*existingGPA
                    df.loc[tempIndex,"Total Grades"] = currTotal + existingTotal
                else:
                    df.loc[tempIndex,"Avg Student GPA"] = 0.0
                    df.loc[tempIndex,"Total Grades"] = 0 
    df = df.drop(df.index[df["Total Grades"] == 0].tolist())      
    df = df.drop(df.index[df["Avg Student GPA"] == "NR"].tolist())  

    return df
   
def compareCourses(df,nCourses,gpa,year):
    nn = NearestNeighbors(n_neighbors=nCourses, algorithm='ball_tree')
    nn.fit(np.array(df))
    dist, ind = nn.kneighbors([[1,1,year,gpa]])
    result = pd.DataFrame({"Course":np.repeat([""],len(ind[0])),"Index":list(ind[0]),"Dist":list(dist[0])})
    return result
        
def chooseCourses(major,gpa,year,nCourses,groupToDepartment,f20,s21,start):
    df = preprocess(major, groupToDepartment, f20, s21)
    dfNN = df.drop(["String","Total Grades"],axis=1)
    dfNN_indeces = []
    for index,row in dfNN.iterrows():
        dfNN_indeces.append(index)
    
    print("Choosing " + str(nCourses) + " courses for {major=" + str(major) + ", GPA=" + str(gpa) + ", year=" + str(year) + "}")
    result = compareCourses(dfNN,nCourses,gpa,year)
    for i,r in result.iterrows():
        result.loc[i,"Course"] = df.loc[dfNN_indeces[int(r["Index"])],"String"]
    result = result.drop("Index",axis=1)

    print("Top " + str(nCourses) + " courses:")
    print(result,"\n")
    end = time.time()
    print("Time (sec): "+str(round(end-start,2)))

def main(major="",gpa=0,year=0,nCourses=5,simulate=False,nSimulations=1):
    start=time.time()
    f20 = pd.read_csv("fall2020.csv")
    s21 = pd.read_csv("spring2021.csv")

    groupToDepartment = {} #Academic Group Code --> [Department Codes]
    for x in [f20.iterrows(),s21.iterrows()]:
        for index,row in x: 
            if row["Academic Group Code"] in groupToDepartment.keys():
                groupToDepartment[row["Academic Group Code"]].add(row["Department Code"])
            else:
                groupToDepartment[row["Academic Group Code"]] = {row["Department Code"]}
    
    if simulate:
        for i in range(nSimulations):
            allMajorCodes = list(set().union(*groupToDepartment.values()))
            major = random.choice(allMajorCodes)
            gpa = round(random.random()*4,2)
            year = random.choice([1,2,3,4])
            chooseCourses(major,gpa,year,nCourses,groupToDepartment,f20,s21,start)
    else:
        chooseCourses(major,gpa,year,nCourses,groupToDepartment,f20,s21,start)
    


if __name__=="__main__":
    main(simulate=True) #Simulate a random student
    #main("ENG",3.62,1) #Manual input (majorCode,GPA,yearAsInteger)

## Example Record
# Institution Code                                         IUBLA
# Inst Description                                   Bloomington
# Term Code                                                 4212
# Term Description                                   Spring 2021
# Session Code                                               13W
# Session Description                              Thirteen Week
# Academic Group Code                                        BUS
# Academic Group Description           Kelley School of Business
# Academic Organization Code                              BL-BUS
# Academic Organization Description                     Business
# Department Code                                            BUS
# Course Subject                                           BUS-C
# Catalog Number                                             204
# Class #                                                   6233
# Course Description                      BUSINESS COMMUNICATION
# Course Topic                                               NaN
# Instructor Name                             Cannon,Jeffrey Day
# GPA Grades                                                  21
# Total Grades                                                24
# Percent Majors                                            95.7
# Avg Class Grade                                           3.69
# Avg Student GPA                                          3.503
# Percent A Grades                                          76.2
# Percent B Grades                                          23.8
# Percent C Grades                                           0.0
# Percent D Grades                                           0.0
# All Other Grades #                                           3
# A+                                                           0
# A                                                            9
# A-                                                           7
# B+                                                           3
# B                                                            1
# B-                                                           1
# C+                                                           0
# C                                                            0
# C-                                                           0
# D+                                                           0
# D                                                            0
# D-                                                           0
# F                                                            0
# P                                                            0
# S                                                            0
# I                                                            0
# R                                                            0
# NY                                                           0
# NR                                                           0
# NC                                                           0
# W                                                            2
# WX                                                           1
# Other                                                        0
# Data Freeze Date                                    06-11-2021
# Unnamed: 51                                                NaN