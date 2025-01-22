from DataBUS import Response


def logging_dict(a_dict, logfile, special_feat=None):
    for key, value in a_dict.items():
        if key == special_feat and special_feat != None:
            logfile.append(f"{special_feat}")
            for i in value:
                logfile.append(f"{i}")
        elif key == "message":
            logfile.append("Message:")
            for i in value:
                logfile.append(f"{i}")
        else:
            logfile.append(f"{key}: {value}")
    return logfile


def logging_response(response, logfile):
    assert isinstance(response, Response), "response needs to be a Response"
    logfile.append(f"{response}")
    return logfile
