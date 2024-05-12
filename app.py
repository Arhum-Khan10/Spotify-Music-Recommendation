from flask import Flask, render_template

app = Flask(__name__)

@app.route('/')
def loading():
    return render_template('loading.html')

@app.route('/landing')
def landing():
    return render_template('landing.html')

@app.route("/index")
def index():
	return render_template("index.html")



if __name__ == '__main__':
    app.run(debug=True)
