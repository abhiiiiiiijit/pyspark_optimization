import  pandas as pd

df = pd.read_csv("/home/adminabhi/gitrepo/marathon_dataset/marathon_dataset.csv",low_memory=False)

## not able to read the file
df.head(5)
