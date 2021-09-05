class OrderScreeningException(Exception):
    pass

class InsufficientBalanceException(OrderScreeningException):
    pass

class InsufficientHoldingException(OrderScreeningException):
    pass

