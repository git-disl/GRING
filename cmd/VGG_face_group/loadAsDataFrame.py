import glob
import os
import pandas as pd

path = "./vgg_face2/n001878/"

li = []
os.chdir(path)
for file in glob.glob("*.jpg"):
    img = file
    # print(path+file)
    li.append(path+file)

ones = ["1"] * len(li)

output = pd.DataFrame(list(zip(li, ones)), columns =['File_Path', 'Labels'])

output.to_csv("../../Path_with_labels4.csv",index=False)
