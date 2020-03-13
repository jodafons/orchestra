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
        # try:
        session = Session()
        if self.username.data == "":
            print ("Username empty")
            return False
        if '.' in self.username.data:
            print ("Username contains dots")
            return False
        if self.email.data == "":
            print ("Email empty")
            return False
        if not self.email.data.endswith('lps.ufrj.br'):
            print ("Not an LPS e-mail")
            return False
        if self.password.data == "":
            print ("PW empty")
            return False
        if self.password_confirm.data == "":
            print ("PWC empty")
            return False
        if not (self.password.data == self.password_confirm.data):
            print ("Passwords don't match")
            return False
        user = session.query(Worker).filter_by(Worker.username == self.username.data).first()
        print (user)
        if user:
            print ("User already exists")
            return False
        else:
            # import hashlib
            # h = hashlib.md5()
            # h.update(self.password.data)
            # passwordHash = h.hexdigest()

            # user = Worker(
            #     username     = self.username.data,
            #     email        = self.email.data,
            #     maxPriority  = 1000,
            #     passwordHash = passwordHash
            # )

            # print (user)

            # session.add(user)

            # session.commit()

            # print ("Success")
            return True
        # except:
        #     print ("Shit it failed")
        # finally:
            session.close()

    def validate_on_submit (self):
        return self.validate()

    def to_dict(self):
        return {
            'username' : self.username.data,
            'password' : self.password.data,
            'email'    : self.email.data,
        }
