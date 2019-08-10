""" Response Object """


class XprResponse:
    def __init__(self, outcome, error_code, results):
        self.outcome = outcome
        self.error_code = error_code
        self.results = results
