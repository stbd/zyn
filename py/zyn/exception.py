
class ZynException(Exception):
    pass


class ZynConnectionLost(ZynException):
    pass


class ZynServerException(ZynException):
    def __init__(self, error_code, description):
        super(ZynServerException, self).__init__(description)
        self.zyn_error_code = error_code
