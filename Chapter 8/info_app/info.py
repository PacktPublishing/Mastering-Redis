from flask import Flask, render_template
import redis

app = Flask(__name__)

redis_db = redis.StrictRedis(host='redis')

@app.route("/")
def default():
    return render_template(
        'index.html', 
        info=redis_db.info())
    
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)
