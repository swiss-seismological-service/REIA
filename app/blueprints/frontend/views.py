from flask import Blueprint, render_template

frontend = Blueprint('frontend', __name__, template_folder='templates')


@frontend.route('/frontend')
def index():
    return render_template('frontend/home.html')
