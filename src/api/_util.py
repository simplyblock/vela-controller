from pydantic import conint

Int64 = conint(ge=-2 ** 63, lt=2 ** 63)
