import daft

db_name="hudidb"
table_name="customers"

path = f"file:///Users/soumilshah/IdeaProjects/SparkProject/tem/{db_name}/{table_name}"

df = daft.read_hudi(path)
df.show()