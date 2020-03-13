from flask_security.forms import RegisterForm, Required, StringField
from orchestra.db.OrchestraDB import OrchestraDB
from orchestra.db.models import Worker

__all__ = [
    'ExtendedRegisterForm'
]

db = OrchestraDB()

class ExtendedRegisterForm(RegisterForm):

    username = StringField('Username', [Required()])

    def validate(self):
        if not self.username:
            return False
        if not self.email:
            return False
        if not self.password:
            return False
        if not self.password_confirm:
            return False
        if not (self.password == self.password_confirm):
            return False
        user = db.getUser(username)
        if user:
            return False
        else:
            import hashlib
            h = hashlib.md5()
            h.update(self.password)
            passwordHash = h.hexdigest()

            user = Worker(
                username     = self.username,
                email        = self.email,
                maxPriority  = 1000,
                passwordHash = passwordHash
            )
        