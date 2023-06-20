from datetime import date


def as_dict(obj):
    # TODO remove from ml delta table
    """ Convert the class object to a dictionary with all members and it's values """
    d = {}
    for k, v in obj.__dict__.items():
        if isinstance(v, dict):
            for h, val in v.items():
                d[f"{k}_{h}"] = val
        elif isinstance(v, (bool, list, date)):
            d[k] = str(v)
        elif isinstance(v, (str, float, int)):
            d[k] = v
        elif isinstance(v, type(None)):
            d[k] = "None"
    return d
