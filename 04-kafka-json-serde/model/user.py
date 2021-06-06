class User(object):
    """
    User record
    """

    def __init__(self, first_name=None, last_name=None, email=None, age=None):
        super().__init__()
        self.first_name = first_name
        self.last_name = last_name
        self.email = email
        self.age = age
