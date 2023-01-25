from flask import Flask, render_template, request
from flask_bootstrap import Bootstrap
import datetime
from hh_api import load_tobase

app = Flask(__name__)


# TODO при обновлении (т.е. если уже был поиск по параметру) происходит умножение, там что то с логикой в базе, он тип дублирует

@app.route("/")
def index():
    return render_template('index.html')


@app.get('/form/')
def form_get():
    return render_template('form.html')


@app.get('/result/')
def form_result():
    return render_template('result.html')


@app.post('/result/')
def form_post():
    text = request.form['text']
    result = []
    tmp = load_tobase(text)
    for t in tmp:
        result.append(f'Должность: {t[1]} Город: {t[2]} Компания: {t[3]} Зарплата: {t[4]} Ссылка: {t[5]}')
    return render_template('result.html', data=result)


if __name__ == "__main__":
    app.run(debug=True)
