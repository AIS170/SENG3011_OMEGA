from flask import Flask, render_template
app = Flask(__name__)

@app.route('/')
def index():
    # only need the file name because render_template knows to look in the directory called "templates"
    return render_template('index.html')

if __name__ == "__main__":
    app.run(host ='0.0.0.0', port=5001, debug = True) 
