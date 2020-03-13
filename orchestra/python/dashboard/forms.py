from flask_security.forms import RegisterForm, Required, StringField
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from orchestra.db.models import Worker, Base

__all__ = [
    'ExtendedRegisterForm'
]

engine = create_engine('postgres://postgres:postgres@localhost:5432/postgres')

Session = sessionmaker(bind=engine)
#Base.metadata.create_all(engine)

class ExtendedRegisterForm(RegisterForm):

    username = StringField('Username', [Required()])

    def validate(self):
        if self.username.value == "":
            print ("Username empty")
            return False
        if self.email.value == "":
            print ("Email empty")
            return False
        if self.password.value == "":
            print ("PW empty")
            return False
        if self.password_confirm.value == "":
            print ("PWC empty")
            return False
        if not (self.password.value == self.password_confirm.value):
            print ("Passwords don't match")
            return False
        user = db.getUser(username)
        print (user)
        if user:
            print ("User already exists")
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

            print (user)

            session.add(user)

            session.commit()
            session.close()

            print ("Success")

            return user

