from flask.ext.wtf import Form, TextField, PasswordField, BooleanField
from flask.ext.wtf import Required

class LoginForm(Form):
    username = TextField('openid', validators = [Required()])
    password = PasswordField('openid', validators = [Required()])
    remember_me = BooleanField('remember_me', default = False)
