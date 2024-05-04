from flask import Flask, render_template, request
import daft

app = Flask(__name__)

global df
db_name = "hudidb"
table_name = "customers"
path = f"file:///Users/soumilshah/IdeaProjects/SparkProject/tem/{db_name}/{table_name}"
df = daft.read_hudi(path)


# Define function to fetch filtered data from Hudi table
def fetch_filtered_data(state):
    filtered_df = df.where((df["state"] == state) )
    filtered_data = filtered_df.to_pandas().to_dict(orient='records')
    return filtered_data


# Define route for home page
@app.route('/')
def home():
    # Fetch all data initially and show only the first two items
    data = df.limit(5).to_pandas().to_dict(orient='records')
    return render_template('index.html', data=data)


# Define route for filtering data based on state and city
@app.route('/filter', methods=['POST'])
def filter_data():
    # Get filter parameters from form
    state = request.form['state']
    # Fetch filtered data based on state and city
    filtered_data = fetch_filtered_data(state)
    # Render template with filtered data
    return render_template('index.html', data=filtered_data)


if __name__ == '__main__':
    app.run(debug=True)
