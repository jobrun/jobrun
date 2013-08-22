from flask.ext.wtf import Form
from wtforms import TextField, PasswordField, BooleanField
from wtforms.validators import Required

class LoginForm(Form):
    username = TextField('openid', validators = [Required()])
    password = PasswordField('openid', validators = [Required()])
    remember_me = BooleanField('remember_me', default = False)
