class OrderScreeningException(Exception):
    pass

class InsuffientBalanceException(OrderScreeningException):
    pass

class InsuffientHoldingException(OrderScreeningException):
    pass

