from multicorn import ForeignDataWrapper
import requests
import json


def run(url):
    r = requests.get(url)
    return {
        "status_code": r.status_code,
        "encoding": r.encoding,
        "resp": json.dumps(r.json()),
        "url": url,
    }


class RequestsFdw(ForeignDataWrapper):

    def __init__(self, options, columns):
        super(RequestsFdw, self).__init__(options, columns)
        self.columns = columns

    def execute(self, quals, columns):
        if not quals:
            return ("No search specified",)
        for qual in quals:
            if qual.field_name == "url" or qual.operator == "=":
                yield run(qual.value)
