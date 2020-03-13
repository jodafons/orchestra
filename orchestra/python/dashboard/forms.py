from flask_security.forms import RegisterForm, Required, StringField
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from orchestra.db.models import Worker, Base

__all__ = [
    'ExtendedRegisterForm'
]

engine = create_engine('postgres://postgres:postgres@localhost:5432/postgres')

Session = sessionmaker(bind=engine)
Base.metadata.create_all(engine)

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

            session = Session()

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

            session.add(user)

            session.commit()
            session.close()
            
            return user
        