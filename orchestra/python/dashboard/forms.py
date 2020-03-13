from flask_security.forms import RegisterForm, Required, StringField

__all__ = [
    'ExtendedRegisterForm'
]

class ExtendedRegisterForm(RegisterForm):
    username = StringField('Username', [Required()])