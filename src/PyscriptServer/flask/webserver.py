'''
from flask import Flask, render_template
app = Flask(__name__)
 
@app.route('/')
def main():
    return render_template('index.html')
 
if __name__ == '__main__':
    app.run()
'''
from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/hello', methods=['GET']) #test api
def hello_world():
    return jsonify({'result':'success','msg':'Hello, World!'})

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5000)
