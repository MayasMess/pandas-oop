class ValidationError(Exception):
    """Empty directory exception"""
    def __init__(self, msg):
        self.msg = msg
        super(ValidationError, self).__init__(msg)


class MissingDecorator(Exception):
    """Empty directory exception"""
    def __init__(self, msg):
        self.msg = msg
        super(MissingDecorator, self).__init__(msg)


class MissingArguments(Exception):
    """Empty directory exception"""
    def __init__(self, msg):
        self.msg = msg
        super(MissingArguments, self).__init__(msg)
